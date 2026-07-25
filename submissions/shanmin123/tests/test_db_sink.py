from datetime import datetime, timezone

from pipeline.db_sink import (
    DAILY_INTERVAL,
    SOURCE,
    get_active_tickers,
    price_rows_to_platform,
    write_prices,
)


class FakeCursor:
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
