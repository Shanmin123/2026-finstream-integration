from datetime import datetime, timezone

from pipeline.checkpoint import CheckpointStore
from pipeline.runner import PipelineRunner
from pipeline.storage import LayeredStorage


class FakeClient:
    def __init__(self):
        self.connect_count = 0
        self.disconnect_count = 0
        self.price_calls = []
        self.news_calls = []

    def connect(self):
        self.connect_count += 1

    def disconnect(self):
        self.disconnect_count += 1

    def fetch_daily_bars(self, symbol, duration, end_datetime="", use_rth=True):
        self.price_calls.append(
            {"symbol": symbol, "duration": duration, "use_rth": use_rth}
        )
        return [
            {
                "event_date": "2025-01-02",
                "symbol": symbol,
                "con_id": 265598,
                "exchange": "SMART",
                "currency": "USD",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000.0,
                "bar_count": 10,
                "wap": 100.2,
            }
        ]

    def list_news_providers(self):
        return [{"code": "BRFG", "name": "Briefing.com General Market Columns"}]

    def fetch_news_headlines(self, symbol, provider_codes, start, end, limit):
        self.news_calls.append(
            {"symbol": symbol, "start": start, "end": end, "limit": limit}
        )
        return [
            {
                "published_at_utc": "2025-01-02T12:00:00+00:00",
                "symbol": symbol,
                "con_id": 265598,
                "provider_code": "BRFG",
                "article_id": "article-1",
                "headline": "Synthetic headline",
                "extra_data": "",
            }
        ]


def make_runner(config, client, storage=None):
    storage = storage or LayeredStorage(
        config.paths.raw_data_dir,
        config.paths.intermediate_data_dir,
        config.paths.output_dir,
    )
    return PipelineRunner(
        config=config,
        client=client,
        storage=storage,
        checkpoint=CheckpointStore(config.paths.checkpoint_file),
        now_fn=lambda: datetime(2025, 1, 3, tzinfo=timezone.utc),
        sleep_fn=lambda _seconds: None,
    )


def test_price_run_updates_checkpoint_only_after_durable_write(pipeline_config):
    client = FakeClient()
    runner = make_runner(pipeline_config, client)

    summary = runner.run_prices()

    assert summary["written"] == 2
    assert summary["errors"] == []
    assert client.connect_count == 1
    assert client.disconnect_count == 1
    assert client.price_calls[0]["duration"] == "3 Y"
    state = CheckpointStore(pipeline_config.paths.checkpoint_file).load()
    assert state["prices"]["AAPL"]["last_event_date"] == "2025-01-02"
    assert state["prices"]["MSFT"]["last_event_date"] == "2025-01-02"


def test_price_resume_requests_only_the_missing_window(pipeline_config):
    checkpoint = CheckpointStore(pipeline_config.paths.checkpoint_file)
    checkpoint.update_price("AAPL", "2025-01-01")
    client = FakeClient()
    runner = make_runner(pipeline_config, client)

    runner.run_prices()

    assert client.price_calls[0]["duration"] == "5 D"


def test_failed_write_does_not_advance_checkpoint(pipeline_config):
    class FailingStorage:
        def write_prices(self, *_args, **_kwargs):
            raise OSError("disk full")

    client = FakeClient()
    runner = make_runner(pipeline_config, client, storage=FailingStorage())

    summary = runner.run_prices()

    assert len(summary["errors"]) == 2
    assert CheckpointStore(pipeline_config.paths.checkpoint_file).load()["prices"] == {}
    assert client.disconnect_count == 1


def test_news_run_uses_checkpoint_overlap_and_advances_after_write(pipeline_config):
    checkpoint = CheckpointStore(pipeline_config.paths.checkpoint_file)
    checkpoint.update_news("AAPL", "2025-01-02T11:00:00+00:00")
    client = FakeClient()
    runner = make_runner(pipeline_config, client)

    summary = runner.run_news()

    assert summary["written"] == 1
    assert client.news_calls[0]["start"] == "20250102 10:55:00"
    state = checkpoint.load()
    assert state["news"]["AAPL"]["last_published_at_utc"] == "2025-01-02T12:00:00+00:00"


def test_smoke_test_is_read_only_and_reports_prices_and_news(pipeline_config):
    client = FakeClient()
    runner = make_runner(pipeline_config, client)

    result = runner.smoke_test()

    assert result == {
        "symbol": "AAPL",
        "price_rows": 1,
        "news_providers": ["BRFG"],
        "headline_rows": 1,
    }
    assert not pipeline_config.paths.checkpoint_file.exists()


def test_inter_symbol_throttle_sleeps_between_symbols(pipeline_config):
    # Full-universe sweeps must pace politely: with N symbols the runner sleeps
    # N-1 times (never before the first request).
    client = FakeClient()
    storage = LayeredStorage(
        pipeline_config.paths.raw_data_dir,
        pipeline_config.paths.intermediate_data_dir,
        pipeline_config.paths.output_dir,
    )
    sleeps = []
    runner = PipelineRunner(
        config=pipeline_config,
        client=client,
        storage=storage,
        checkpoint=CheckpointStore(pipeline_config.paths.checkpoint_file),
        now_fn=lambda: datetime(2025, 1, 3, tzinfo=timezone.utc),
        sleep_fn=sleeps.append,
    )
    runner.run_prices()
    n_symbols = len(pipeline_config.prices.symbols)
    delay = pipeline_config.run.symbol_delay_seconds
    expected = [delay] * (n_symbols - 1) if delay > 0 else []
    assert sleeps == expected


class FakeStreamClient(FakeClient):
    """Emits one batch of ticks per drain, then goes quiet."""

    def __init__(self, batches):
        super().__init__()
        self.batches = list(batches)
        self.started = None
        self.stopped = 0

    def start_quote_stream(self, symbols, market_data_type=3):
        self.started = (list(symbols), market_data_type)
        return [1, 2]

    def drain_ticks(self):
        return self.batches.pop(0) if self.batches else []

    def stop_quote_stream(self):
        self.stopped += 1


def _tick(symbol, field, value, date="2026-07-25"):
    return {
        "tick_time_utc": date + "T14:00:00+00:00",
        "symbol": symbol,
        "field": field,
        "value": value,
    }


def test_stream_pipeline_is_tick_driven_and_writes_each_flush(pipeline_config, tmp_path):
    import dataclasses

    pipeline_config = dataclasses.replace(
        pipeline_config,
        stream=dataclasses.replace(
            pipeline_config.stream, duration_seconds=1, flush_interval_seconds=0.1
        ),
    )
    # Seed a price history so live features have warm state.
    storage = LayeredStorage(
        pipeline_config.paths.raw_data_dir,
        pipeline_config.paths.intermediate_data_dir,
        pipeline_config.paths.output_dir,
    )
    import pandas as pd

    dates = pd.date_range("2026-01-02", periods=40, freq="B").strftime("%Y-%m-%d")
    rows = [
        {
            "event_date": d, "symbol": "AAPL", "con_id": 1, "exchange": "SMART",
            "currency": "USD", "open": 100.0 + i, "high": 101.0 + i, "low": 99.0 + i,
            "close": 100.5 + i, "volume": 1000.0, "bar_count": 5, "wap": 100.2 + i,
        }
        for i, d in enumerate(dates)
    ]
    storage.write_prices(rows, {"symbol": "AAPL"}, "2026-07-24T00:00:00+00:00", "seed")

    client = FakeStreamClient(
        [
            [_tick("AAPL", "last", 150.0), _tick("AAPL", "bid", 149.9)],
            [],                                   # a quiet interval writes nothing
            [_tick("AAPL", "last", 151.0)],
        ]
    )
    runner = PipelineRunner(
        config=pipeline_config,
        client=client,
        storage=storage,
        checkpoint=CheckpointStore(pipeline_config.paths.checkpoint_file),
        now_fn=lambda: datetime(2026, 7, 25, 14, 5, tzinfo=timezone.utc),
        sleep_fn=lambda _s: None,
    )
    result = runner.run_stream_pipeline()

    assert client.started[0] == list(pipeline_config.stream.symbols)
    assert client.connect_count == 1 and client.disconnect_count == 1
    assert client.stopped == 1                      # subscription cancelled on exit
    assert result["ticks"] == 3                     # quiet interval contributed nothing
    assert result["flushes"] == 2                   # only non-empty drains flush
    assert result["quote_rows"] >= 3
    assert result["live_feature_rows"] > 0

    live = list(
        (tmp_path / "final" / "indicators_live").glob(
            "event_year=*/symbol=*/part-000.parquet"
        )
    ) if (tmp_path / "final").exists() else list(
        (storage.final_dir / "indicators_live").glob(
            "event_year=*/symbol=*/part-000.parquet"
        )
    )
    assert live, "expected provisional live indicators on disk"
    frame = pd.read_parquet(live[0])
    # The last flush wins: the newest streamed price drives the provisional row.
    assert bool(frame.iloc[-1]["provisional"]) is True
