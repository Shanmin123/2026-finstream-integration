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

    def run_stream(self):
        if not self.config.stream.symbols:
            raise RuntimeError(
                "stream.symbols is empty: list the symbols to subscribe to in the "
                "config's stream section."
            )

        def stream():
            rows = self.client.stream_quotes(
                self.config.stream.symbols,
                self.config.stream.duration_seconds,
                market_data_type=self.config.stream.market_data_type,
            )
            retrieved_at = self._utc_now()
            request = {
                "symbols": list(self.config.stream.symbols),
                "duration_seconds": self.config.stream.duration_seconds,
                "market_data_type": self.config.stream.market_data_type,
            }
            result = self.storage.write_quotes(
                rows,
                request,
                retrieved_at.isoformat(),
                self._run_id("quotes", "stream", retrieved_at),
            )
            self.logger.info(
                "dataset_write_complete",
                extra={"dataset": "quotes", "symbol": "*", "rows": result.final_rows},
            )
            return {
                "tick_rows": len(rows),
                "final_rows": result.final_rows,
                "symbols": list(self.config.stream.symbols),
                "duration_seconds": self.config.stream.duration_seconds,
            }

        return self._run_connected(stream)

    def run_stream_pipeline(self):
        """Continuous streaming pipeline: ticks in, features out, until stopped.

        One long-running process. The daily price history is loaded once as warm
        state; the quote subscription then drives everything. Every
        ``stream.flush_interval_seconds`` the buffered ticks are appended to the
        quotes dataset and the provisional live features are recomputed for the
        symbols that moved. Writes are micro-batched at the flush interval rather
        than per tick because the sink is partitioned parquet; the trigger is the
        tick flow, not a schedule. Runs until ``stream.duration_seconds``
        (0 = until interrupted).
        """
        from pipeline.features import compute_live_features

        prices = self.storage.read_final_prices()
        if prices.empty:
            raise RuntimeError(
                "No final prices found. Run collect-prices before stream-pipeline."
            )
        # Full history is kept on purpose: OBV is cumulative and EMA/MACD and
        # Wilder RSI/ATR are recursive, so a bounded window silently changes
        # their values. The per-flush cost is controlled by only recomputing
        # the symbols that ticked, not by truncating their history.
        warm = prices
        cfg = self.config.stream
        symbols = list(cfg.symbols)
        if not symbols:
            raise RuntimeError(
                "stream.symbols is empty: list the symbols to subscribe to in the "
                "config's stream section."
            )
        state = {"ticks": 0, "quote_rows": 0, "feature_rows": 0, "flushes": 0}
        # Latest observed value per (symbol, field): the provisional bar carries
        # the real streamed open/high/low/volume, never a fabricated one.
        bars = {}

        def flush():
            rows = self.client.drain_ticks()
            if not rows:
                return
            moved = set()
            now = self._utc_now()
            state["ticks"] += len(rows)
            state["flushes"] += 1
            result = self.storage.write_quotes(
                rows,
                {"symbols": symbols, "market_data_type": cfg.market_data_type},
                now.isoformat(),
                self._run_id("quotes", "stream", now),
            )
            state["quote_rows"] += result.final_rows
            for row in rows:
                field = row["field"]
                # `close` is IB's previous-session close, not today's price.
                if field not in ("last", "open", "high", "low", "volume"):
                    continue
                event_date = row.get("session_date") or row["tick_time_utc"][:10]
                bar = bars.setdefault(row["symbol"], {})
                if bar.get("event_date") != event_date:
                    bar.clear()          # new session: never mix days in one bar
                    bar["event_date"] = event_date
                bar["close" if field == "last" else field] = row["value"]
                moved.add(row["symbol"])
            # Only the symbols that actually ticked in THIS flush, and only the
            # current session: a persistent bars dict would keep re-emitting a
            # stale row for a symbol that has stopped trading.
            current = max(
                (b["event_date"] for b in bars.values() if b.get("event_date")),
                default=None,
            )
            priced = {
                symbol: bar
                for symbol, bar in bars.items()
                if symbol in moved
                and bar.get("close") is not None
                and bar.get("event_date") == current
            }
            if not priced:
                return
            # Recompute over the moved symbols only: passing the whole warm
            # panel made every flush sort and group all 658 symbols, which on a
            # full universe costs more than the flush interval itself.
            subset = warm[warm["symbol"].isin(priced)]
            missing = sorted(set(priced) - set(subset["symbol"].unique()))
            if missing:
                self.logger.warning(
                    "live_features_without_price_history",
                    extra={
                        "dataset": "indicators_live",
                        "symbol": ",".join(missing),
                        "rows": 0,
                    },
                )
            live_i, live_a = compute_live_features(subset, priced, now.isoformat())
            for dataset, frame in (
                ("indicators_live", live_i),
                ("alphas_live", live_a),
            ):
                written = self.storage.write_derived(
                    dataset, frame, ("symbol", "event_date"), "as_of_utc"
                )
                state["feature_rows"] += written.final_rows
            self.logger.info(
                "stream_flush",
                extra={
                    "dataset": "quotes+live_features",
                    "symbol": "*",
                    "rows": len(rows),
                },
            )

        def pipeline():
            self.client.start_quote_stream(
                symbols, market_data_type=cfg.market_data_type
            )
            self.logger.info(
                "stream_started",
                extra={"dataset": "quotes", "symbol": ",".join(symbols), "rows": 0},
            )
            deadline = (
                time.time() + cfg.duration_seconds if cfg.duration_seconds else None
            )
            try:
                while deadline is None or time.time() < deadline:
                    error = getattr(self.client, "stream_error", lambda: None)()
                    if error is not None:
                        raise error
                    # Never sleep past the deadline: a flush interval longer
                    # than the run would otherwise overshoot it.
                    nap = cfg.flush_interval_seconds
                    if deadline is not None:
                        nap = min(nap, max(0.0, deadline - time.time()))
                    self.sleep_fn(nap)
                    flush()
                    if deadline is not None and time.time() >= deadline:
                        break
            except KeyboardInterrupt:
                self.logger.info(
                    "stream_interrupted",
                    extra={"dataset": "quotes", "symbol": "*", "rows": 0},
                )
            finally:
                # Drain before cancelling: stop_quote_stream drops the request
                # mapping, so ticks decoded after it would be discarded. The
                # cancel itself must happen even if that drain fails.
                try:
                    flush()
                finally:
                    self.client.stop_quote_stream()
                flush()
                self.logger.info(
                    "stream_stopped",
                    extra={"dataset": "quotes", "symbol": "*", "rows": state["ticks"]},
                )
            return {
                "symbols": symbols,
                "ticks": state["ticks"],
                "quote_rows": state["quote_rows"],
                "live_feature_rows": state["feature_rows"],
                "flushes": state["flushes"],
                "market_data_type": cfg.market_data_type,
            }

        return self._run_connected(pipeline)

    def run_live_features(self):
        from pipeline.features import compute_live_features

        prices = self.storage.read_final_prices()
        if prices.empty:
            raise RuntimeError(
                "No final prices found. Run collect-prices before compute-live-features."
            )
        bars = self.storage.read_latest_quote_bars()
        if not bars:
            raise RuntimeError(
                "No streamed quotes found. Run stream-quotes before compute-live-features."
            )
        as_of = self._utc_now().isoformat()
        live_i, live_a = compute_live_features(prices, bars, as_of)
        summary = {}
        for dataset, frame in (("indicators_live", live_i), ("alphas_live", live_a)):
            result = self.storage.write_derived(
                dataset, frame, ("symbol", "event_date"), "as_of_utc"
            )
            summary[dataset] = {"rows": result.final_rows, "symbols": len(frame)}
            self.logger.info(
                "dataset_write_complete",
                extra={"dataset": dataset, "symbol": "*", "rows": result.final_rows},
            )
        summary["as_of_utc"] = as_of
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
