import json

import pandas as pd
import pytest

from pipeline.storage import LayeredStorage


def test_price_write_preserves_raw_and_upserts_final_rows(tmp_path):
    storage = LayeredStorage(
        raw_dir=tmp_path / "raw",
        intermediate_dir=tmp_path / "processed",
        final_dir=tmp_path / "output",
    )
    request = {"symbol": "AAPL", "duration": "7 D"}
    rows = [
        {
            "event_date": "2025-01-02",
            "symbol": "AAPL",
            "con_id": 265598,
            "exchange": "SMART",
            "currency": "USD",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 1000.0,
            "bar_count": 10,
            "wap": 100.5,
        },
        {
            "event_date": "2025-01-02",
            "symbol": "AAPL",
            "con_id": 265598,
            "exchange": "SMART",
            "currency": "USD",
            "open": 100.0,
            "high": 103.0,
            "low": 99.0,
            "close": 102.0,
            "volume": 1100.0,
            "bar_count": 11,
            "wap": 101.0,
        },
    ]

    first = storage.write_prices(rows, request, "2025-01-03T00:00:00+00:00", "run-1")
    second = storage.write_prices(
        rows[-1:], request, "2025-01-03T01:00:00+00:00", "run-2"
    )

    assert first.input_rows == 2
    assert second.final_rows == 1
    raw_payload = json.loads(first.raw_path.read_text(encoding="utf-8").splitlines()[0])
    assert raw_payload["request"] == request
    final = pd.read_parquet(second.final_paths[0])
    assert list(final["close"]) == [102.0]
    assert list(final["retrieved_at_utc"]) == ["2025-01-03T01:00:00+00:00"]


def test_news_write_deduplicates_by_symbol_provider_and_article(tmp_path):
    storage = LayeredStorage(
        raw_dir=tmp_path / "raw",
        intermediate_dir=tmp_path / "processed",
        final_dir=tmp_path / "output",
    )
    rows = [
        {
            "published_at_utc": "2025-01-02T12:00:00+00:00",
            "symbol": "AAPL",
            "con_id": 265598,
            "provider_code": "BRFG",
            "article_id": "article-1",
            "headline": "Synthetic headline v1",
        },
        {
            "published_at_utc": "2025-01-02T12:00:00+00:00",
            "symbol": "AAPL",
            "con_id": 265598,
            "provider_code": "BRFG",
            "article_id": "article-1",
            "headline": "Synthetic headline v2",
        },
    ]

    result = storage.write_news(
        rows,
        {"symbol": "AAPL", "providers": ["BRFG"]},
        "2025-01-02T13:00:00+00:00",
        "run-news",
    )

    final = pd.read_parquet(result.final_paths[0])
    assert result.final_rows == 1
    assert list(final["headline"]) == ["Synthetic headline v2"]
    assert "article_text" not in final.columns


def _quote(symbol, field, value, when):
    return {"tick_time_utc": when, "symbol": symbol, "field": field, "value": value}


def test_quote_bars_use_one_session_and_ignore_the_previous_close_tick(tmp_path):
    storage = LayeredStorage(tmp_path / "raw", tmp_path / "mid", tmp_path / "final")
    rows = [
        _quote("AAPL", "high", 999.0, "2026-07-24T20:00:00+00:00"),
        _quote("AAPL", "last", 200.0, "2026-07-24T20:00:00+00:00"),
        _quote("AAPL", "last", 250.0, "2026-07-25T14:00:00+00:00"),
        _quote("AAPL", "high", 252.0, "2026-07-25T14:00:01+00:00"),
        # IB's `close` tick is the PREVIOUS session's close; it must not become
        # today's provisional close.
        _quote("AAPL", "close", 199.0, "2026-07-25T14:00:02+00:00"),
    ]
    storage.write_quotes(rows, {}, "2026-07-25T14:01:00+00:00", "bars-test")
    bar = storage.read_latest_quote_bars()["AAPL"]
    assert bar["event_date"] == "2026-07-25"
    assert bar["close"] == 250.0        # from `last`, not from the close tick
    assert bar["high"] == 252.0         # today's high, never yesterday's 999


def test_raw_microbatches_within_one_second_do_not_overwrite(tmp_path):
    storage = LayeredStorage(tmp_path / "raw", tmp_path / "mid", tmp_path / "final")
    first = storage.write_quotes(
        [_quote("AAPL", "last", 1.0, "2026-07-25T14:00:00.100000+00:00")],
        {}, "2026-07-25T14:00:00.100000+00:00", "quotes-stream-20260725T140000Z",
    )
    second = storage.write_quotes(
        [_quote("AAPL", "last", 2.0, "2026-07-25T14:00:00.600000+00:00")],
        {}, "2026-07-25T14:00:00.600000+00:00", "quotes-stream-20260725T140000Z",
    )
    assert first.raw_path != second.raw_path
    assert first.raw_path.exists() and second.raw_path.exists()


def test_legacy_quotes_without_session_date_do_not_win_the_session_max(tmp_path):
    """A null session_date stringified to "nan" and beat a real date in max(),
    returning a stale bar under event_date="nan"."""
    storage = LayeredStorage(tmp_path / "raw", tmp_path / "mid", tmp_path / "final")
    legacy = {"tick_time_utc": "2026-07-24T20:00:00+00:00", "symbol": "AAPL",
              "field": "last", "value": 200.0}                      # no session_date
    current = {"tick_time_utc": "2026-07-25T14:00:00+00:00", "session_date": "2026-07-25",
               "symbol": "AAPL", "field": "last", "value": 250.0}
    storage.write_quotes([legacy], {}, "2026-07-24T20:01:00+00:00", "legacy")
    storage.write_quotes([current], {}, "2026-07-25T14:01:00+00:00", "current")
    bar = storage.read_latest_quote_bars()["AAPL"]
    assert bar["event_date"] == "2026-07-25"
    assert bar["close"] == 250.0


def test_latest_year_only_reads_just_the_newest_year_partition(tmp_path):
    """The daily loader only needs the newest session. Reading every year would
    make its parquet I/O grow with accumulated history."""
    storage = LayeredStorage(
        raw_dir=tmp_path / "raw",
        intermediate_dir=tmp_path / "processed",
        final_dir=tmp_path / "output",
    )
    frame = pd.DataFrame(
        [
            {"symbol": "AAA", "event_date": "2024-06-03", "rsi_14": 30.0,
             "as_of_utc": "2024-06-03T00:00:00+00:00"},
            {"symbol": "AAA", "event_date": "2026-07-24", "rsi_14": 55.0,
             "as_of_utc": "2026-07-24T00:00:00+00:00"},
            {"symbol": "BBB", "event_date": "2026-07-24", "rsi_14": 60.0,
             "as_of_utc": "2026-07-24T00:00:00+00:00"},
        ]
    )
    storage.write_derived("indicators", frame, ("symbol", "event_date"), "as_of_utc")

    everything = storage.read_final_dataset("indicators")
    assert sorted(everything["event_date"]) == ["2024-06-03", "2026-07-24", "2026-07-24"]

    newest = storage.read_final_dataset("indicators", latest_year_only=True)
    assert sorted(newest["event_date"]) == ["2026-07-24", "2026-07-24"]
    assert sorted(newest["symbol"]) == ["AAA", "BBB"]


def test_latest_year_only_refuses_partitions_that_are_not_four_digit_years(tmp_path):
    """A malformed event_date yields a year directory that does not sort like a
    year, which would silently hide the real newest session."""
    storage = LayeredStorage(
        raw_dir=tmp_path / "raw",
        intermediate_dir=tmp_path / "processed",
        final_dir=tmp_path / "output",
    )
    frame = pd.DataFrame(
        [
            {"symbol": "AAA", "event_date": "2026-07-24", "rsi_14": 55.0,
             "as_of_utc": "2026-07-24T00:00:00+00:00"},
            {"symbol": "AAA", "event_date": "oops-07-24", "rsi_14": 10.0,
             "as_of_utc": "2026-07-24T00:00:00+00:00"},
        ]
    )
    storage.write_derived("indicators", frame, ("symbol", "event_date"), "as_of_utc")

    # Reading everything still works; only the newest-year shortcut refuses.
    assert len(storage.read_final_dataset("indicators")) == 2
    with pytest.raises(ValueError, match="non-YYYY year partitions"):
        storage.read_final_dataset("indicators", latest_year_only=True)


def test_latest_year_only_rejects_unicode_digit_years(tmp_path):
    """str.isdigit() accepts non-ASCII digits, which sort AFTER the ASCII
    years and would silently win max() over the real newest partition."""
    storage = LayeredStorage(
        raw_dir=tmp_path / "raw",
        intermediate_dir=tmp_path / "processed",
        final_dir=tmp_path / "output",
    )
    frame = pd.DataFrame(
        [
            {"symbol": "AAA", "event_date": "2026-07-24", "rsi_14": 55.0,
             "as_of_utc": "2026-07-24T00:00:00+00:00"},
            {"symbol": "AAA", "event_date": "٢٠٢٧-07-24",
             "rsi_14": 10.0, "as_of_utc": "2026-07-24T00:00:00+00:00"},
        ]
    )
    storage.write_derived("indicators", frame, ("symbol", "event_date"), "as_of_utc")
    with pytest.raises(ValueError, match="non-YYYY year partitions"):
        storage.read_final_dataset("indicators", latest_year_only=True)


def test_latest_year_only_opens_only_newest_year_files(tmp_path, monkeypatch):
    """The point of the flag is bounded parquet I/O: older year partitions must
    not be read and filtered afterwards."""
    storage = LayeredStorage(
        raw_dir=tmp_path / "raw",
        intermediate_dir=tmp_path / "processed",
        final_dir=tmp_path / "output",
    )
    frame = pd.DataFrame(
        [
            {"symbol": "AAA", "event_date": "2024-06-03", "rsi_14": 30.0,
             "as_of_utc": "2024-06-03T00:00:00+00:00"},
            {"symbol": "AAA", "event_date": "2026-07-24", "rsi_14": 55.0,
             "as_of_utc": "2026-07-24T00:00:00+00:00"},
        ]
    )
    storage.write_derived("indicators", frame, ("symbol", "event_date"), "as_of_utc")

    opened = []
    real_read = pd.read_parquet

    def spy(path, *args, **kwargs):
        opened.append(str(path))
        return real_read(path, *args, **kwargs)

    monkeypatch.setattr("pipeline.storage.pd.read_parquet", spy)
    newest = storage.read_final_dataset("indicators", latest_year_only=True)
    assert list(newest["event_date"]) == ["2026-07-24"]
    assert opened and all("event_year=2026" in path for path in opened)


def test_replayed_older_write_does_not_clobber_the_newer_value(tmp_path):
    """Dedup must key on order_column, not arrival order: an at-least-once
    replay of an older batch used to overwrite the newer merged value."""
    storage = LayeredStorage(tmp_path / "raw", tmp_path / "mid", tmp_path / "final")
    row = {
        "event_date": "2025-01-02", "symbol": "AAPL", "con_id": 1,
        "exchange": "SMART", "currency": "USD", "open": 1.0, "high": 1.0,
        "low": 1.0, "close": 111.0, "volume": 1.0, "bar_count": 1, "wap": 1.0,
    }
    storage.write_prices([row], {}, "2025-01-03T02:00:00+00:00", "newer")
    stale = dict(row, close=99.0)
    storage.write_prices([stale], {}, "2025-01-03T01:00:00+00:00", "replay")
    final = storage.read_final_prices()
    assert list(final["close"]) == [111.0]


def test_equal_order_stamp_ties_keep_the_incoming_row(tmp_path):
    """Same retrieved_at on both sides: the incoming write must win the tie,
    preserving the correct-a-bad-value-now overwrite path."""
    storage = LayeredStorage(tmp_path / "raw", tmp_path / "mid", tmp_path / "final")
    row = {
        "event_date": "2025-01-02", "symbol": "AAPL", "con_id": 1,
        "exchange": "SMART", "currency": "USD", "open": 1.0, "high": 1.0,
        "low": 1.0, "close": 100.0, "volume": 1.0, "bar_count": 1, "wap": 1.0,
    }
    stamp = "2025-01-03T00:00:00+00:00"
    storage.write_prices([row], {}, stamp, "first")
    storage.write_prices([dict(row, close=101.0)], {}, stamp, "second")
    assert list(storage.read_final_prices()["close"]) == [101.0]


def test_price_volume_is_stored_as_double(tmp_path):
    """The published Spark schema says double; ibapi 9.81 delivers int
    volumes and 10.x Decimals, and mixed physical schemas across partitions
    would break a schema-applying reader."""
    storage = LayeredStorage(tmp_path / "raw", tmp_path / "mid", tmp_path / "final")
    row = {
        "event_date": "2025-01-02", "symbol": "AAPL", "con_id": 1,
        "exchange": "SMART", "currency": "USD", "open": 1.0, "high": 1.0,
        "low": 1.0, "close": 100.0, "volume": 1000, "bar_count": 1, "wap": 1.0,
    }
    result = storage.write_prices([row], {}, "2025-01-03T00:00:00+00:00", "r")
    import pyarrow.parquet as pq

    schema = pq.read_schema(result.final_paths[0])
    assert str(schema.field("volume").type) == "double"
