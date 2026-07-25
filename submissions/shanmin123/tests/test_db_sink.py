from datetime import datetime, timezone

from pipeline.db_sink import (
    DAILY_INTERVAL,
    SOURCE,
    get_active_tickers,
    price_rows_to_platform,
    write_prices,
)


class FakeCursor:
    rowcount = 1                       # psycopg2 reports rows actually inserted

    def __init__(self, store, rows=None):
        self.store = store
        self._rows = rows or []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self.store.append(("execute", sql, params))

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, store, rows=None):
        self.store = store
        self._rows = rows or []
        self.committed = 0
        self.closed = 0

    def cursor(self):
        return FakeCursor(self.store, self._rows)

    def commit(self):
        self.committed += 1

    def close(self):
        self.closed += 1


def test_price_rows_map_onto_the_platform_contract():
    rows = [
        {
            "event_date": "2026-07-24", "symbol": "AAPL", "open": 1.0, "high": 2.0,
            "low": 0.5, "close": 1.5, "volume": 100.0,
        }
    ]
    mapped = price_rows_to_platform(rows)
    ticker, timestamp_ms, datetime_utc, o, h, l, c, v, interval, source = mapped[0]
    assert ticker == "AAPL"
    expected_ms = int(
        datetime(2026, 7, 24, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert timestamp_ms == expected_ms          # conflict key component
    assert datetime_utc.startswith("2026-07-24T00:00:00")
    assert (o, h, l, c, v) == (1.0, 2.0, 0.5, 1.5, 100.0)
    assert interval == DAILY_INTERVAL and source == SOURCE


def test_write_prices_upserts_idempotently_and_closes(monkeypatch):
    calls = []
    conn = FakeConn(calls)
    captured = {}

    def fake_execute_values(cur, sql, values):
        captured["sql"] = sql
        captured["values"] = values

    monkeypatch.setattr("psycopg2.extras.execute_values", fake_execute_values,
                        raising=False)
    sent = write_prices(
        [{"event_date": "2026-07-24", "symbol": "AAPL", "open": 1.0, "high": 2.0,
          "low": 0.5, "close": 1.5, "volume": 100.0}],
        connection_factory=lambda: conn,
    )
    assert sent == 1
    assert "ON CONFLICT (ticker, timestamp_ms, interval) DO NOTHING" in captured["sql"]
    assert conn.committed == 1 and conn.closed == 1


def test_write_prices_is_a_noop_without_rows():
    assert write_prices([], connection_factory=lambda: None) == 0


def test_active_tickers_fall_back_to_none_when_db_is_unreachable():
    def boom():
        raise OSError("no database")

    assert get_active_tickers(connection_factory=boom) is None


def test_active_tickers_reads_the_platform_company_table():
    calls = []
    conn = FakeConn(calls, rows=[("AAPL",), ("MSFT",)])
    assert get_active_tickers(connection_factory=lambda: conn) == ["AAPL", "MSFT"]
    assert "is_active = TRUE" in calls[0][1]


def test_quote_rows_map_with_latency_provenance():
    from pipeline.db_sink import quote_rows_to_platform

    rows = [{"tick_time_utc": "2026-07-25T14:00:00+00:00", "symbol": "AAPL",
             "field": "last", "value": 250.25}]
    ticker, timestamp_ms, datetime_utc, field, value, mdt, source = (
        quote_rows_to_platform(rows, market_data_type=3)[0]
    )
    assert (ticker, field, value, mdt, source) == ("AAPL", "last", 250.25, 3, SOURCE)
    assert timestamp_ms == int(
        datetime(2026, 7, 25, 14, 0, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert datetime_utc.startswith("2026-07-25T14:00:00")


def test_derived_frame_melts_to_long_rows_and_drops_warmup_nans():
    import numpy as np
    import pandas as pd

    from pipeline.db_sink import derived_frame_to_platform

    frame = pd.DataFrame(
        [
            {"symbol": "AAPL", "event_date": "2026-07-24", "rsi_14": 65.2,
             "sma_50": np.nan, "computed_at_utc": "2026-07-25T00:00:00+00:00"},
        ]
    )
    mapped = derived_frame_to_platform(frame)
    assert len(mapped) == 1                      # the NaN warm-up cell is skipped
    ticker, _ms, _dt, name, value, interval, provisional, computed_at, source = mapped[0]
    assert (ticker, name, value, interval, provisional, source) == (
        "AAPL", "rsi_14", 65.2, "1d", False, SOURCE,
    )
    assert computed_at == "2026-07-25T00:00:00+00:00"


def test_live_frame_is_flagged_provisional_and_stamped_from_as_of():
    import pandas as pd

    from pipeline.db_sink import derived_frame_to_platform

    frame = pd.DataFrame(
        [{"symbol": "AAPL", "event_date": "2026-07-25", "rsi_14": 66.0,
          "computed_at_utc": "2026-07-25T13:00:00+00:00",
          "as_of_utc": "2026-07-25T14:30:00+00:00", "provisional": True}]
    )
    row = derived_frame_to_platform(frame)[0]
    assert row[6] is True                        # is_provisional
    assert row[7] == "2026-07-25T14:30:00+00:00"  # newest refresh wins the upsert


def test_indicator_upsert_lets_the_newer_computation_win(monkeypatch):
    import pandas as pd

    from pipeline.db_sink import write_indicators

    calls = []
    conn = FakeConn(calls)
    captured = {}
    monkeypatch.setattr(
        "psycopg2.extras.execute_values",
        lambda cur, sql, values: captured.update(sql=sql, values=values),
        raising=False,
    )
    frame = pd.DataFrame(
        [{"symbol": "AAPL", "event_date": "2026-07-24", "rsi_14": 65.2,
          "computed_at_utc": "2026-07-25T00:00:00+00:00"}]
    )
    assert write_indicators(frame, connection_factory=lambda: conn) == 1
    sql = captured["sql"]
    assert "ON CONFLICT (ticker, timestamp_ms, indicator_name, interval, source)" in sql
    assert "DO UPDATE SET" in sql
    assert "EXCLUDED.computed_at_utc >= technical_indicators.computed_at_utc" in sql


def test_derived_upsert_refuses_to_downgrade_a_final_value(monkeypatch):
    import pandas as pd

    from pipeline.db_sink import write_indicators

    captured = {}
    monkeypatch.setattr(
        "psycopg2.extras.execute_values",
        lambda cur, sql, values: captured.update(sql=sql),
        raising=False,
    )
    frame = pd.DataFrame(
        [{"symbol": "AAPL", "event_date": "2026-07-24", "rsi_14": 65.2,
          "computed_at_utc": "2026-07-25T00:00:00+00:00"}]
    )
    write_indicators(frame, connection_factory=lambda: FakeConn([]))
    sql = captured["sql"]
    # A newer provisional row must not overwrite a final one.
    assert "EXCLUDED.is_provisional = FALSE OR technical_indicators.is_provisional = TRUE" in sql


def test_price_write_reports_rows_actually_inserted(monkeypatch):
    """price_data's unique key excludes source, so an existing bar wins; the
    write must report what really landed rather than what was sent."""
    from pipeline.db_sink import write_prices

    class ConflictCursor(FakeCursor):
        rowcount = 0                      # every row conflicted

    class ConflictConn(FakeConn):
        def cursor(self):
            return ConflictCursor(self.store)

    monkeypatch.setattr(
        "psycopg2.extras.execute_values", lambda *_a, **_k: None, raising=False
    )
    sent = write_prices(
        [{"event_date": "2026-07-24", "symbol": "AAPL", "open": 1.0, "high": 2.0,
          "low": 0.5, "close": 1.5, "volume": 100.0}],
        connection_factory=lambda: ConflictConn([]),
    )
    assert sent == 0
