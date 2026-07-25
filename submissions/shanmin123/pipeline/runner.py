import logging
import time
from datetime import datetime, timedelta, timezone


class PipelineRunner:
    def __init__(self, config, client, storage, checkpoint, now_fn=None, sleep_fn=None):
        self.config = config
        self.client = client
        self.storage = storage
        self.checkpoint = checkpoint
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self.sleep_fn = sleep_fn or time.sleep
        self.logger = logging.getLogger("ibkr_pipeline")

    def run_prices(self):
        return self._run_connected(self._collect_prices)

    def run_news(self):
        return self._run_connected(self._collect_news)

    def run_all(self):
        def collect_all():
            return {
                "prices": self._collect_prices(),
                "news": self._collect_news(),
            }

        return self._run_connected(collect_all)

    def smoke_test(self):
        def smoke():
            now = self._utc_now()
            symbol = self.config.prices.symbols[0]
            bars = self._with_retries(
                "smoke_prices",
                symbol,
                lambda: self.client.fetch_daily_bars(
                    symbol,
                    "7 D",
                    use_rth=self.config.prices.use_rth,
                ),
            )
            providers = self._with_retries(
                "smoke_news_providers",
                symbol,
                self.client.list_news_providers,
            )
            available = [provider["code"] for provider in providers]
            selected = [
                code for code in self.config.news.providers if code in available
            ]
            headlines = []
            if selected:
                start = self._format_ib_time(
                    now - timedelta(days=self.config.news.lookback_days)
                )
                headlines = self._with_retries(
                    "smoke_news",
                    symbol,
                    lambda: self.client.fetch_news_headlines(
                        symbol,
                        selected,
                        start,
                        self._format_ib_time(now),
                        self.config.news.limit,
                    ),
                )
            return {
                "symbol": symbol,
                "price_rows": len(bars),
                "news_providers": available,
                "headline_rows": len(headlines),
            }

        return self._run_connected(smoke)

    def _run_connected(self, operation):
        self.logger.info("gateway_connect_start")
        self.client.connect()
        try:
            self.logger.info("gateway_connected")
            return operation()
        finally:
            self.client.disconnect()
            self.logger.info("gateway_disconnected")

    def _collect_prices(self):
        summary = {"written": 0, "errors": []}
        state = self.checkpoint.load()
        for index, symbol in enumerate(self.config.prices.symbols):
            if index and self.config.run.symbol_delay_seconds > 0:
                # Polite inter-symbol pacing so a full S&P-500 sweep stays well
                # under IBKR historical-data pacing limits.
                self.sleep_fn(self.config.run.symbol_delay_seconds)
            duration = self._price_duration(state, symbol)
            request = {
                "symbol": symbol,
                "duration": duration,
                "bar_size": "1 day",
                "what_to_show": "TRADES",
                "use_rth": self.config.prices.use_rth,
            }
            try:
                rows = self._with_retries(
                    "collect_prices",
                    symbol,
                    lambda symbol=symbol, duration=duration: (
                        self.client.fetch_daily_bars(
                            symbol,
                            duration,
                            use_rth=self.config.prices.use_rth,
                        )
                    ),
                )
                retrieved_at = self._utc_now()
                self.storage.write_prices(
                    rows,
                    request,
                    retrieved_at.isoformat(),
                    self._run_id("prices", symbol, retrieved_at),
                )
                if rows:
                    last_date = max(row["event_date"] for row in rows)
                    self.checkpoint.update_price(symbol, last_date)
                    state.setdefault("prices", {})[symbol] = {
                        "last_event_date": last_date
                    }
                summary["written"] += len(rows)
                self.logger.info(
                    "dataset_write_complete",
                    extra={"dataset": "prices", "symbol": symbol, "rows": len(rows)},
                )
            except Exception as exc:
                summary["errors"].append(
                    {"symbol": symbol, "error": str(exc), "type": type(exc).__name__}
                )
                self.logger.exception(
                    "dataset_collection_failed",
                    extra={"dataset": "prices", "symbol": symbol},
                )
                if not self.config.run.continue_on_error:
                    raise
        return summary

    def _collect_news(self):
        summary = {"written": 0, "errors": []}
        state = self.checkpoint.load()
        for index, symbol in enumerate(self.config.news.symbols):
            if index and self.config.run.symbol_delay_seconds > 0:
                self.sleep_fn(self.config.run.symbol_delay_seconds)
            now = self._utc_now()
            start = self._news_start(state, symbol, now)
            request = {
                "symbol": symbol,
                "provider_codes": list(self.config.news.providers),
                "start": self._format_ib_time(start),
                "end": self._format_ib_time(now),
                "limit": self.config.news.limit,
            }
            try:
                rows = self._with_retries(
                    "collect_news",
                    symbol,
                    lambda symbol=symbol, request=request: (
                        self.client.fetch_news_headlines(
                            symbol,
                            request["provider_codes"],
                            request["start"],
                            request["end"],
                            request["limit"],
                        )
                    ),
                )
                retrieved_at = self._utc_now()
                self.storage.write_news(
                    rows,
                    request,
                    retrieved_at.isoformat(),
                    self._run_id("news", symbol, retrieved_at),
                )
                if rows:
                    last_time = max(row["published_at_utc"] for row in rows)
                    self.checkpoint.update_news(symbol, last_time)
                    state.setdefault("news", {})[symbol] = {
                        "last_published_at_utc": last_time
                    }
                summary["written"] += len(rows)
                self.logger.info(
                    "dataset_write_complete",
                    extra={"dataset": "news", "symbol": symbol, "rows": len(rows)},
                )
            except Exception as exc:
                summary["errors"].append(
                    {"symbol": symbol, "error": str(exc), "type": type(exc).__name__}
                )
                self.logger.exception(
                    "dataset_collection_failed",
                    extra={"dataset": "news", "symbol": symbol},
                )
                if not self.config.run.continue_on_error:
                    raise
        return summary

    def run_features(self):
        """Compute technical indicators and the Alpha101 subset (offline).

        Reads the pipeline's own final prices dataset; no Gateway connection.
        Recomputable by design, so there is no checkpoint: the partition merge
        keeps (symbol, event_date) unique.
        """
        from pipeline.features import compute_alphas, compute_indicators

        prices = self.storage.read_final_prices()
        if prices.empty:
            raise RuntimeError(
                "No final prices found. Run collect-prices before compute-features."
            )
        computed_at = self._utc_now().isoformat()
        summary = {}
        for dataset, frame in (
            ("indicators", compute_indicators(prices, computed_at)),
            ("alphas", compute_alphas(prices, computed_at)),
        ):
            result = self.storage.write_derived(
                dataset,
                frame,
                ("symbol", "event_date"),
                "computed_at_utc",
            )
            summary[dataset] = {
                "input_price_rows": len(prices),
                "rows": result.final_rows,
                "partitions": len(result.final_paths),
            }
            self.logger.info(
                "dataset_write_complete",
                extra={"dataset": dataset, "symbol": "*", "rows": result.final_rows},
            )
        return summary

    def _with_retries(self, operation, symbol, callback):
        last_error = None
        attempts = self.config.run.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                self.logger.info(
                    "ib_request_start",
                    extra={
                        "operation": operation,
                        "symbol": symbol,
                        "attempt": attempt,
                    },
                )
                return callback()
            except Exception as exc:
                last_error = exc
                self.logger.warning(
                    "ib_request_retry",
                    extra={
                        "operation": operation,
                        "symbol": symbol,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )
                if attempt < attempts:
                    self.sleep_fn(self.config.run.retry_delay_seconds)
        raise last_error

    def _price_duration(self, state, symbol):
        item = state.get("prices", {}).get(symbol)
        if not item:
            return self.config.prices.duration
        last_date = datetime.strptime(item["last_event_date"], "%Y-%m-%d").date()
        missing_days = (self._utc_now().date() - last_date).days
        return "{0} D".format(max(1, missing_days + 3))

    def _news_start(self, state, symbol, now):
        item = state.get("news", {}).get(symbol)
        if not item:
            return now - timedelta(days=self.config.news.lookback_days)
        value = item["last_published_at_utc"]
        checkpoint_time = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if checkpoint_time.tzinfo is None:
            checkpoint_time = checkpoint_time.replace(tzinfo=timezone.utc)
        return checkpoint_time.astimezone(timezone.utc) - timedelta(
            minutes=self.config.news.overlap_minutes
        )

    def _utc_now(self):
        value = self.now_fn()
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _format_ib_time(value):
        return value.astimezone(timezone.utc).strftime("%Y%m%d %H:%M:%S")

    @staticmethod
    def _run_id(dataset, symbol, value):
        return "{0}-{1}-{2}".format(
            dataset,
            symbol,
            value.strftime("%Y%m%dT%H%M%SZ"),
        )
