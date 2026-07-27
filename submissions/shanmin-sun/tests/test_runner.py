from datetime import datetime, timezone

import pytest

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

    def fetch_news_headlines(self, symbol, provider_codes, start, end, limit,
                             since_utc=None, max_pages=5):
        self.news_calls.append(
            {"symbol": symbol, "start": start, "end": end, "limit": limit,
             "since_utc": since_utc, "max_pages": max_pages}
        )
        return [
            {
                "published_at_utc": "2025-01-02T12:00:00+00:00",
                "symbol": symbol,
                "con_id": 265598,
                "provider_code": "BRFG",
                "article_id": "article-1",
                "headline": "Synthetic headline",
            }
        ], True


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
    # The DB loader pushes from this low-water mark forward: everything the
    # run fetched, not one panel-wide max date.
    assert summary["earliest_event_date"] == "2025-01-02"
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
    # reqHistoricalNews paginates backwards from the start anchor, so both
    # bounds are "now"; the checkpoint (minus overlap) bounds the backwards
    # walk instead of the request window.
    assert client.news_calls[0]["start"] == "2025-01-03 00:00:00.0"
    assert client.news_calls[0]["end"] == client.news_calls[0]["start"]
    assert client.news_calls[0]["since_utc"] == "2025-01-02T10:55:00+00:00"
    assert client.news_calls[0]["max_pages"] == 5
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

    def stream_error(self):
        return None


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
            pipeline_config.stream,
            symbols=("AAPL",),
            duration_seconds=1,
            flush_interval_seconds=0.1,
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


def test_stream_recomputes_only_symbols_that_ticked(pipeline_config, tmp_path):
    """A symbol that stops trading must not keep getting fresh feature rows,
    and the recomputation must not scan the whole universe every flush."""
    import dataclasses

    import pandas as pd

    storage = LayeredStorage(
        pipeline_config.paths.raw_data_dir,
        pipeline_config.paths.intermediate_data_dir,
        pipeline_config.paths.output_dir,
    )
    dates = pd.date_range("2026-01-02", periods=40, freq="B").strftime("%Y-%m-%d")
    for symbol in ("AAA", "BBB"):
        storage.write_prices(
            [
                {
                    "event_date": d, "symbol": symbol, "con_id": 1,
                    "exchange": "SMART", "currency": "USD", "open": 100.0 + i,
                    "high": 101.0 + i, "low": 99.0 + i, "close": 100.5 + i,
                    "volume": 1000.0, "bar_count": 5, "wap": 100.2 + i,
                }
                for i, d in enumerate(dates)
            ],
            {"symbol": symbol}, "2026-07-24T00:00:00+00:00", "seed-" + symbol,
        )

    def tick(symbol, value):
        return {
            "tick_time_utc": "2026-07-25T14:00:00+00:00",
            "session_date": "2026-07-25", "symbol": symbol,
            "field": "last", "value": value,
        }

    # Both move, then only AAA moves.
    client = FakeStreamClient([[tick("AAA", 150.0), tick("BBB", 90.0)], [tick("AAA", 151.0)]])
    config = dataclasses.replace(
        pipeline_config,
        stream=dataclasses.replace(
            pipeline_config.stream, symbols=("AAA", "BBB"),
            duration_seconds=1, flush_interval_seconds=0.1,
        ),
    )
    runner = PipelineRunner(
        config=config, client=client, storage=storage,
        checkpoint=CheckpointStore(config.paths.checkpoint_file),
        now_fn=lambda: datetime(2026, 7, 25, 14, 5, tzinfo=timezone.utc),
        sleep_fn=lambda _s: None,
    )
    # Observe what each flush actually recomputes: the stored dataset is
    # deduplicated per symbol/date, so it cannot distinguish "BBB was skipped"
    # from "BBB was recomputed and overwrote itself".
    from pipeline.features import compute_live_features as real_compute

    recomputed = []

    def spy(prices, bars, as_of):
        recomputed.append(sorted(bars))
        assert set(prices["symbol"].unique()) <= set(bars), (
            "the recomputation must be scoped to the symbols passed in"
        )
        return real_compute(prices, bars, as_of)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("pipeline.features.compute_live_features", spy)
    try:
        result = runner.run_stream_pipeline()
    finally:
        monkeypatch.undo()
    assert result["ticks"] == 3
    assert recomputed == [["AAA", "BBB"], ["AAA"]], (
        "the second flush must recompute only the symbol that ticked"
    )
    live = storage.read_final_dataset("indicators_live")
    assert set(live["symbol"]) == {"AAA", "BBB"}


def test_news_window_uses_the_format_reqhistoricalnews_accepts():
    """IB ignored the compact bar-style timestamp and returned headlines far
    outside the requested window; historical news wants YYYY-MM-DD HH:MM:SS.0."""
    from pipeline.runner import PipelineRunner

    formatted = PipelineRunner._format_ib_time(
        datetime(2026, 7, 23, 2, 25, 50, tzinfo=timezone.utc)
    )
    assert formatted == "2026-07-23 02:25:50.0"


def _seed_price_history(storage, *symbols):
    """Write 40 business days of bars so live features have warm state."""
    import pandas as pd

    dates = pd.date_range("2026-01-02", periods=40, freq="B").strftime("%Y-%m-%d")
    for symbol in symbols:
        storage.write_prices(
            [
                {
                    "event_date": d, "symbol": symbol, "con_id": 1,
                    "exchange": "SMART", "currency": "USD", "open": 100.0 + i,
                    "high": 101.0 + i, "low": 99.0 + i, "close": 100.5 + i,
                    "volume": 1000.0, "bar_count": 5, "wap": 100.2 + i,
                }
                for i, d in enumerate(dates)
            ],
            {"symbol": symbol}, "2026-07-24T00:00:00+00:00", "seed-" + symbol,
        )


def _stream_runner(pipeline_config, client, storage, sleep_fn=lambda _s: None):
    import dataclasses

    config = dataclasses.replace(
        pipeline_config,
        stream=dataclasses.replace(
            pipeline_config.stream, symbols=("AAA",),
            duration_seconds=1, flush_interval_seconds=0.1,
        ),
    )
    return PipelineRunner(
        config=config, client=client, storage=storage,
        checkpoint=CheckpointStore(config.paths.checkpoint_file),
        now_fn=lambda: datetime(2026, 7, 25, 14, 5, tzinfo=timezone.utc),
        sleep_fn=sleep_fn,
    )


def _stream_storage(pipeline_config):
    storage = LayeredStorage(
        pipeline_config.paths.raw_data_dir,
        pipeline_config.paths.intermediate_data_dir,
        pipeline_config.paths.output_dir,
    )
    _seed_price_history(storage, "AAA")
    return storage


def _aaa_tick(value, at="14:00:00"):
    return {
        "tick_time_utc": "2026-07-25T" + at + "+00:00",
        "session_date": "2026-07-25", "symbol": "AAA",
        "field": "last", "value": value,
    }


def test_stream_sink_receives_only_the_current_flush_rows(pipeline_config):
    """The DB loader must forward each flush's own rows. Re-reading the session
    partitions afterwards would re-send the whole trading day on every
    scheduled run."""
    storage = _stream_storage(pipeline_config)
    client = FakeStreamClient([[_aaa_tick(150.0)], [_aaa_tick(151.0)]])
    runner = _stream_runner(pipeline_config, client, storage)
    seen = []
    runner.run_stream_pipeline(sink=lambda ticks, i, a: seen.append(len(ticks)))
    # Two flushes of one tick each -- never the accumulated two.
    assert seen == [1, 1]


def test_shutdown_retries_a_failed_batch_and_drains_the_rest(pipeline_config):
    """A sink failure keeps its batch pending: the post-cancel drain retries it
    together with whatever arrived in between (at-least-once, the conflict
    clauses absorb the replay), and the sink error still surfaces."""

    def interrupt(_seconds):
        raise KeyboardInterrupt

    storage = _stream_storage(pipeline_config)
    client = FakeStreamClient([[_aaa_tick(150.0)], [_aaa_tick(151.0)]])
    runner = _stream_runner(pipeline_config, client, storage, sleep_fn=interrupt)
    seen = []

    def sink(ticks, _indicators, _alphas):
        seen.append([row["value"] for row in ticks])
        if len(seen) == 1:
            raise RuntimeError("sink down")

    with pytest.raises(RuntimeError, match="sink down"):
        runner.run_stream_pipeline(sink=sink)
    assert client.stopped == 1
    # The failed batch is retried with the newly drained tick, not dropped.
    assert seen == [[150.0], [150.0, 151.0]]


def test_interrupt_inside_a_flush_does_not_lose_the_drained_batch(pipeline_config):
    """Ctrl-C landing inside the write path used to be read as a graceful stop
    while the already-drained ticks vanished: they were out of the buffer but
    not yet in parquet. The pending batch must survive into the shutdown
    drains and the run must still report what it durably flushed."""
    import pandas as pd

    storage = _stream_storage(pipeline_config)
    client = FakeStreamClient(
        [[_aaa_tick(150.0)], [_aaa_tick(151.0, at="14:00:05")]]
    )
    runner = _stream_runner(pipeline_config, client, storage)

    real_write = storage.write_quotes
    calls = {"n": 0}

    def interrupted_write(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise KeyboardInterrupt
        return real_write(*args, **kwargs)

    storage.write_quotes = interrupted_write
    result = runner.run_stream_pipeline()
    assert result["ticks"] == 2
    assert client.stopped == 1
    quotes = pd.concat(
        [pd.read_parquet(p) for p in storage.final_dir.glob("quotes/**/*.parquet")],
        ignore_index=True,
    )
    assert sorted(quotes["value"]) == [150.0, 151.0]


def test_stream_failure_is_not_replaced_by_a_shutdown_failure(pipeline_config):
    """When the stream itself died, the caller must see that error even if a
    shutdown drain also fails; the drain failure is secondary and logged."""

    class DeadStreamClient(FakeStreamClient):
        def stream_error(self):
            return RuntimeError("stream died")

    storage = _stream_storage(pipeline_config)
    client = DeadStreamClient([[_aaa_tick(150.0)]])
    runner = _stream_runner(pipeline_config, client, storage)

    def sink(_ticks, _indicators, _alphas):
        raise ValueError("sink down")

    with pytest.raises(RuntimeError, match="stream died"):
        runner.run_stream_pipeline(sink=sink)
    assert client.stopped == 1


def test_disconnect_failure_does_not_replace_the_run_error(pipeline_config):
    """The outer disconnect is the last cleanup to run; if it also fails, the
    stream's own error must still be the one the caller sees."""

    class DeadStreamFailingDisconnect(FakeStreamClient):
        def stream_error(self):
            return RuntimeError("stream died")

        def disconnect(self):
            super().disconnect()
            raise OSError("disconnect failed")

    storage = _stream_storage(pipeline_config)
    client = DeadStreamFailingDisconnect([])
    runner = _stream_runner(pipeline_config, client, storage)
    with pytest.raises(RuntimeError, match="stream died"):
        runner.run_stream_pipeline()
    assert client.disconnect_count == 1


def test_disconnect_failure_alone_still_raises(pipeline_config):
    class FailingDisconnect(FakeClient):
        def disconnect(self):
            super().disconnect()
            raise OSError("disconnect failed")

    runner = make_runner(pipeline_config, FailingDisconnect())
    with pytest.raises(OSError, match="disconnect failed"):
        runner.run_prices()


def test_rejection_arriving_at_shutdown_is_not_erased(pipeline_config):
    """stop_quote_stream clears per-request errors, so a rejection landing
    between the loop's last check and shutdown used to vanish and the run
    read as a success. The shutdown now checks once more AFTER the first
    drain (data already safe) and surfaces it."""

    class LateRejectClient(FakeStreamClient):
        def __init__(self, batches):
            super().__init__(batches)
            self.checks = 0

        def stream_error(self):
            self.checks += 1
            if self.checks >= 2:
                return RuntimeError("354 not subscribed")
            return None

    def interrupt(_seconds):
        raise KeyboardInterrupt

    storage = _stream_storage(pipeline_config)
    client = LateRejectClient([[_aaa_tick(150.0)], [_aaa_tick(151.0, at="14:00:05")]])
    runner = _stream_runner(pipeline_config, client, storage, sleep_fn=interrupt)
    with pytest.raises(RuntimeError, match="354"):
        runner.run_stream_pipeline()
    assert client.stopped == 1
    import pandas as pd

    quotes = pd.concat(
        [pd.read_parquet(p) for p in storage.final_dir.glob("quotes/**/*.parquet")],
        ignore_index=True,
    )
    # Both drains ran before the error surfaced: nothing was lost to it.
    assert sorted(quotes["value"]) == [150.0, 151.0]


def test_truncated_news_walk_holds_the_checkpoint_and_reports_the_gap(pipeline_config):
    """Every walk anchors at "now", so when the page budget runs out with IB
    still flagging older unread headlines, NO checkpoint value makes the next
    default run reach deeper: advancing would skip the remainder forever and
    a fake step downward would livelock. The checkpoint holds, the gap is
    logged, and news.max_pages is the operator's lever."""
    import logging

    class TruncatedNewsClient(FakeClient):
        def fetch_news_headlines(self, symbol, *_args, **_kwargs):
            rows = [
                {
                    "published_at_utc": "2025-01-02T12:00:00+00:00",
                    "symbol": symbol, "con_id": 1, "provider_code": "BRFG",
                    "article_id": "new", "headline": "newest",
                },
                {
                    "published_at_utc": "2025-01-02T09:00:00+00:00",
                    "symbol": symbol, "con_id": 1, "provider_code": "BRFG",
                    "article_id": "old", "headline": "oldest collected",
                },
            ]
            return rows, False   # page budget spent, more remain below

    checkpoint = CheckpointStore(pipeline_config.paths.checkpoint_file)
    checkpoint.update_news("AAPL", "2025-01-01T08:00:00+00:00")
    client = TruncatedNewsClient()
    runner = make_runner(pipeline_config, client)
    # A direct handler: the pipeline logger does not propagate once
    # configure_logging has run anywhere in the session, so caplog would
    # miss it.
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    pipeline_logger = logging.getLogger("ibkr_pipeline")
    pipeline_logger.addHandler(handler)
    try:
        runner.run_news()
    finally:
        pipeline_logger.removeHandler(handler)
    state = checkpoint.load()
    assert (
        state["news"]["AAPL"]["last_published_at_utc"]
        == "2025-01-01T08:00:00+00:00"
    ), "an uncovered interval must not advance the checkpoint"
    assert any(
        record.msg == "news_backlog_exceeds_page_budget" for record in records
    )
