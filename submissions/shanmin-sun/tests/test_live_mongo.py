"""The Mongo sink's conflict policies, against a live MongoDB.

The rest of the suite drives the sink with injected fakes, which proves the
right operation is built but not that a server honours it. The derived upsert is
an aggregation-pipeline update: whether MongoDB evaluates that guard the way it
reads is a question only a real server answers, and getting it wrong loses a
newer computation silently.

Skipped unless a server is reachable, so `pytest tests/` stays green without
one. Run it with a local mongod up:

    MONGO_HOST=127.0.0.1 MONGO_PORT=27017 python -m pytest tests/test_live_mongo.py -v

It writes to a throwaway database of its own, dropped afterwards, and never
touches the configured one.
"""
import functools
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

MONGO_HOST = os.getenv("MONGO_HOST", "127.0.0.1")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))

STAMP = datetime(2026, 7, 25, tzinfo=timezone.utc)
DAY = "2026-07-24"
TICKER = "AAPL"


@functools.lru_cache(maxsize=1)
def _server_answers(host, port, timeout=2.0):
    """Probed once per session: without a server every test would otherwise
    pay the connect timeout again."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def indicator_frame(value, computed, provisional=False):
    return pd.DataFrame([{
        "symbol": TICKER, "event_date": DAY, "rsi_14": value,
        "computed_at_utc": computed.isoformat(), "provisional": provisional,
    }])


@pytest.fixture
def database(monkeypatch):
    """A throwaway database on the live server, dropped when the test ends."""
    pytest.importorskip("pymongo")
    if not _server_answers(MONGO_HOST, MONGO_PORT):
        pytest.skip(f"no MongoDB answering on {MONGO_HOST}:{MONGO_PORT}")

    name = f"ibkr_sink_test_{uuid.uuid4().hex[:8]}"
    for key, value in (("MONGO_HOST", MONGO_HOST), ("MONGO_PORT", str(MONGO_PORT)),
                       ("MONGO_DB", name)):
        monkeypatch.setenv(key, value)
    for key in ("MONGO_URI", "MONGO_USER", "MONGO_PASSWORD", "MONGO_SSH_TUNNEL"):
        monkeypatch.delenv(key, raising=False)

    from pipeline import mongo_sink

    db = mongo_sink.get_database()
    mongo_sink.ensure_indexes(lambda: db)
    yield db
    db.client.drop_database(name)


def stored_value(database):
    from pipeline import mongo_sink

    document = database[mongo_sink.INDICATOR_COLLECTION].find_one(
        {"ticker": TICKER, "indicator_name": "rsi_14"}
    )
    return document["value"] if document else None


def test_the_server_accepts_the_pipeline_update(database):
    """The guard is an aggregation-pipeline update. A server that rejected the
    shape would fail here rather than at the first real load."""
    from pipeline.mongo_sink import write_indicators

    assert write_indicators(indicator_frame(50.0, STAMP)) == 1
    assert stored_value(database) == 50.0


def test_a_newer_computation_replaces_an_older_one(database):
    from pipeline.mongo_sink import write_indicators

    write_indicators(indicator_frame(50.0, STAMP))
    write_indicators(indicator_frame(99.0, STAMP + timedelta(days=1)))
    assert stored_value(database) == 99.0


def test_an_older_computation_does_not(database):
    """The direction of the comparison, on the server rather than in a dict."""
    from pipeline.mongo_sink import write_indicators

    write_indicators(indicator_frame(99.0, STAMP))
    write_indicators(indicator_frame(11.0, STAMP - timedelta(days=30)))
    assert stored_value(database) == 99.0


def test_a_provisional_value_cannot_replace_a_final_one(database):
    from pipeline.mongo_sink import write_indicators

    write_indicators(indicator_frame(99.0, STAMP))
    write_indicators(
        indicator_frame(55.5, STAMP + timedelta(days=2), provisional=True)
    )
    assert stored_value(database) == 99.0


def test_an_existing_bar_is_not_overwritten(database):
    """price_data is the platform's contract and this feed is a gap filler."""
    from pipeline.mongo_sink import PRICE_COLLECTION, write_prices

    first = [{"event_date": DAY, "symbol": TICKER, "open": 1.0, "high": 2.0,
              "low": 0.5, "close": 1.5, "volume": 100.0}]
    assert write_prices(first) == 1
    second = [dict(first[0], close=12345.0)]
    assert write_prices(second) == 0
    stored = database[PRICE_COLLECTION].find_one({"ticker": TICKER})
    assert stored["close"] == 1.5


def test_dates_are_stored_as_dates(database):
    """A string date sorts lexically and breaks every range query on it."""
    from pipeline.mongo_sink import PRICE_COLLECTION, write_prices

    write_prices([{"event_date": DAY, "symbol": TICKER, "open": 1.0, "high": 2.0,
                   "low": 0.5, "close": 1.5, "volume": 100.0}])
    stored = database[PRICE_COLLECTION].find_one({"ticker": TICKER})
    assert isinstance(stored["datetime_utc"], datetime)


def test_the_conflict_keys_are_enforced_by_a_unique_index(database):
    from pipeline import mongo_sink

    for collection, key in (
        (mongo_sink.PRICE_COLLECTION, mongo_sink.PRICE_KEY),
        (mongo_sink.INDICATOR_COLLECTION, mongo_sink.INDICATOR_KEY),
        (mongo_sink.ALPHA_COLLECTION, mongo_sink.ALPHA_KEY),
    ):
        indexes = database[collection].index_information()
        unique = [i for i in indexes.values() if i.get("unique")]
        assert unique, f"{collection} has no unique index"
        fields = {field for i in unique for field, _ in i["key"]}
        assert fields == set(key), f"{collection}: {sorted(fields)}"
