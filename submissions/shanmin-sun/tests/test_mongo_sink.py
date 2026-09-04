from datetime import datetime, timezone

import pandas as pd
import pytest
from pymongo.errors import BulkWriteError

from pipeline.db_sink import DAILY_INTERVAL, SOURCE
from pipeline.mongo_sink import (
    ALPHA_COLLECTION,
    ALPHA_KEY,
    INDICATOR_COLLECTION,
    INDICATOR_KEY,
    PRICE_COLLECTION,
    PRICE_KEY,
    QUOTE_COLLECTION,
    QUOTE_KEY,
    FieldContractError,
    build_uri,
    derived_documents,
    ensure_indexes,
    price_documents,
    record_run,
    write_alphas,
    write_indicators,
    write_prices,
    write_quotes,
)


class FakeResult:
    def __init__(self, upserted=0, modified=0):
        self.upserted_count = upserted
        self.modified_count = modified


class FakeCollection:
    def __init__(self, store, name, result=None, rows=None, raises=None):
        self.store = store
        self.name = name
        self._result = result or FakeResult()
        self._rows = rows or []
        self._raises = raises

    def bulk_write(self, operations, ordered=True):
        if self._raises is not None:
            raise self._raises
        self.store.append((self.name, "bulk_write", operations, ordered))
        return self._result

    def insert_one(self, document):
        self.store.append((self.name, "insert_one", document))

    def create_index(self, keys, **kwargs):
        self.store.append((self.name, "create_index", keys, kwargs))
        return kwargs.get("name", "idx")

    def find(self, query, projection=None):
        self.store.append((self.name, "find", query, projection))
        return FakeCursor(self._rows)


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def sort(self, *_args):
        return self

    def __iter__(self):
        return iter(self._rows)


class FakeDatabase:
    def __init__(self, store, result=None, rows=None, raises=None):
        self.store = store
        self._result = result
        self._rows = rows
        self._raises = raises

    def __getitem__(self, name):
        return FakeCollection(self.store, name, self._result, self._rows, self._raises)


def database_of(store, **kwargs):
    return lambda: FakeDatabase(store, **kwargs)


PRICE_ROW = {
    "event_date": "2026-07-24", "symbol": "AAPL", "open": 1.0, "high": 2.0,
    "low": 0.5, "close": 1.5, "volume": 100.0,
}


def indicator_frame(value=55.0, computed="2026-07-25T00:00:00+00:00",
                    provisional=False):
    return pd.DataFrame(
        [{
            "symbol": "AAPL", "event_date": "2026-07-24", "rsi_14": value,
            "computed_at_utc": computed, "provisional": provisional,
        }]
    )


# --- row to document -------------------------------------------------------

def test_price_documents_carry_the_sql_field_names():
    document = price_documents([PRICE_ROW])[0]
    assert set(document) == {
        "ticker", "timestamp_ms", "datetime_utc", "open", "high", "low",
        "close", "volume", "interval", "source",
    }
    assert document["ticker"] == "AAPL"
    assert document["timestamp_ms"] == int(
        datetime(2026, 7, 24, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert document["interval"] == DAILY_INTERVAL and document["source"] == SOURCE


def test_datetime_fields_are_bson_dates_not_strings():
    """A string date sorts lexically and breaks every range query on it."""
    document = price_documents([PRICE_ROW])[0]
    assert isinstance(document["datetime_utc"], datetime)
    assert document["datetime_utc"].tzinfo is not None

    derived = derived_documents(indicator_frame(), "indicator_name")[0]
    assert isinstance(derived["datetime_utc"], datetime)
    assert isinstance(derived["computed_at_utc"], datetime)


def test_derived_documents_name_their_own_key_column():
    indicator = derived_documents(indicator_frame(), "indicator_name")[0]
    assert indicator["indicator_name"] == "rsi_14"
    assert "alpha_id" not in indicator

    alphas = pd.DataFrame([{
        "symbol": "AAPL", "event_date": "2026-07-24", "alpha_6": -0.5,
        "computed_at_utc": "2026-07-25T00:00:00+00:00", "provisional": False,
    }])
    assert derived_documents(alphas, "alpha_id")[0]["alpha_id"] == "alpha_6"


def test_a_mapper_that_grows_a_column_fails_loudly(monkeypatch):
    """The two sinks share db_sink's mappers. If one grows a field and this
    module does not follow, the write must raise rather than store a document
    with a value silently dropped off the end."""
    monkeypatch.setattr(
        "pipeline.mongo_sink.price_rows_to_platform",
        lambda rows: [("AAPL", 1, "2026-07-24T00:00:00+00:00", 1.0)],
    )
    with pytest.raises(FieldContractError):
        price_documents([PRICE_ROW])


def test_non_finite_values_never_reach_the_database():
    assert derived_documents(indicator_frame(float("nan")), "indicator_name") == []
    assert derived_documents(indicator_frame(float("inf")), "indicator_name") == []


# --- conflict policy -------------------------------------------------------

def test_prices_do_not_overwrite_the_primary_feed():
    """price_data's SQL policy is DO NOTHING, so the document already there
    wins and this feed stays a gap filler."""
    store = []
    write_prices([PRICE_ROW], database_of(store, result=FakeResult(upserted=1)))
    collection, _, operations, ordered = store[0]
    assert collection == PRICE_COLLECTION and ordered is False
    operation = operations[0]
    assert set(operation._filter) == set(PRICE_KEY)
    assert set(operation._doc) == {"$setOnInsert"}
    assert operation._upsert is True
    # The key fields come from the filter on an upsert; naming them again in
    # the operator asks the server to write a path the filter already fixed.
    assert not set(operation._doc["$setOnInsert"]) & set(PRICE_KEY)
    # Everything else must be there, or an inserted bar would be missing a price.
    assert set(operation._doc["$setOnInsert"]) == {
        "datetime_utc", "open", "high", "low", "close", "volume", "source",
    }


def test_quotes_are_immutable_observations():
    store = []
    rows = [{
        "symbol": "AAPL", "tick_time_utc": "2026-07-24T13:31:00+00:00",
        "field": "last", "value": 1.5,
    }]
    write_quotes(rows, 3, database_of(store, result=FakeResult(upserted=1)))
    collection, _, operations, _ = store[0]
    assert collection == QUOTE_COLLECTION
    assert set(operations[0]._doc) == {"$setOnInsert"}
    assert set(operations[0]._doc["$setOnInsert"]) == {
        "datetime_utc", "value", "market_data_type",
    }


def test_derived_writes_are_guarded_pipeline_updates():
    """A read-then-write would let two collectors interleave and lose the
    newer value, so the guard has to run inside the update."""
    store = []
    write_indicators(
        indicator_frame(), database_factory=database_of(store, result=FakeResult(1))
    )
    collection, _, operations, _ = store[0]
    assert collection == INDICATOR_COLLECTION
    operation = operations[0]
    assert set(operation._filter) == set(INDICATOR_KEY)
    assert isinstance(operation._doc, list), "must be an aggregation pipeline"
    stage = operation._doc[0]["$set"]
    # Key fields are the filter's job; only the mutable ones are conditional.
    assert set(stage) == {"value", "datetime_utc", "is_provisional", "computed_at_utc"}
    assert all("$cond" in expression for expression in stage.values())


def test_the_newer_computation_wins_and_not_the_older_one():
    """The direction of the timestamp comparison, pinned on its own.

    The structural test above passes whichever way it points, and an inverted
    guard is silent: stale values would overwrite fresh ones and every write
    would still report success.
    """
    store = []
    frame = indicator_frame(computed="2026-07-25T00:00:00+00:00")
    write_indicators(
        frame, database_factory=database_of(store, result=FakeResult(1))
    )
    incoming = derived_documents(frame, "indicator_name")[0]["computed_at_utc"]
    condition = store[0][2][0]._doc[0]["$set"]["value"]["$cond"][0]
    newer = condition["$or"][1]["$and"][0]
    assert newer == {"$gte": [incoming, "$computed_at_utc"]}, (
        "the incoming stamp must be on the left of $gte: the write applies when "
        f"the incoming computation is the newer one, got {newer}"
    )


def test_a_provisional_value_cannot_replace_a_final_one():
    store = []
    write_indicators(
        indicator_frame(provisional=True),
        database_factory=database_of(store, result=FakeResult(1)),
    )
    condition = store[0][2][0]._doc[0]["$set"]["value"]["$cond"][0]
    branches = condition["$or"]
    # Either the document is absent, or it is older AND not being downgraded.
    assert {"$eq": [{"$type": "$computed_at_utc"}, "missing"]} in branches
    guard = branches[1]["$and"][1]["$or"]
    assert {"$eq": [True, False]} in guard          # incoming is provisional
    assert {"$eq": ["$is_provisional", True]} in guard   # so the stored one must be too


def test_alphas_use_their_own_collection_and_key():
    store = []
    frame = pd.DataFrame([{
        "symbol": "AAPL", "event_date": "2026-07-24", "alpha_101": 0.25,
        "computed_at_utc": "2026-07-25T00:00:00+00:00", "provisional": False,
    }])
    write_alphas(frame, database_factory=database_of(store, result=FakeResult(1)))
    collection, _, operations, _ = store[0]
    assert collection == ALPHA_COLLECTION
    assert set(operations[0]._filter) == set(ALPHA_KEY)


# --- counts, failures, plumbing -------------------------------------------

def test_write_reports_what_actually_landed():
    store = []
    written = write_prices(
        [PRICE_ROW, dict(PRICE_ROW, symbol="MSFT")],
        database_of(store, result=FakeResult(upserted=1, modified=0)),
    )
    assert written == 1, "the row the primary feed already had is not counted"


def test_empty_input_never_opens_a_connection():
    def explode():
        raise AssertionError("must not connect for an empty batch")

    assert write_prices([], explode) == 0
    assert write_indicators(pd.DataFrame(), database_factory=explode) == 0


def test_duplicate_keys_are_the_policy_working_not_a_failure():
    error = BulkWriteError({
        "writeErrors": [{"code": 11000}], "nUpserted": 2, "nModified": 0,
    })
    assert write_prices([PRICE_ROW], database_of([], raises=error)) == 2


def test_any_other_bulk_error_still_surfaces():
    error = BulkWriteError({"writeErrors": [{"code": 121}], "nUpserted": 0})
    with pytest.raises(BulkWriteError):
        write_prices([PRICE_ROW], database_of([], raises=error))


def test_every_conflict_key_gets_a_unique_index():
    store = []
    ensure_indexes(database_of(store))
    unique = {
        name: call[3]
        for name, *call in [(c[0], *c) for c in store]
        if call[1] == "create_index" and call[3].get("unique")
    }
    assert set(unique) == {
        PRICE_COLLECTION, QUOTE_COLLECTION, INDICATOR_COLLECTION, ALPHA_COLLECTION,
    }


def test_run_records_carry_a_start_derived_from_latency():
    store = []
    record_run("ibkr_prices", 10, 5.0, "success", database_factory=database_of(store))
    _, _, document = store[0]
    assert document["records_ingested"] == 10 and document["status"] == "success"
    assert (document["end_time"] - document["start_time"]).total_seconds() == 5.0


def test_the_indicator_collection_does_not_default_to_the_shared_one():
    """The fundamentals submission writes EODHD indicators to
    technical_indicators in a wide layout keyed without a source, so an IBKR
    value for a ticker-day would overwrite one of theirs. Pointing this at that
    collection has to be a decision, not what a first run does."""
    assert INDICATOR_COLLECTION != "technical_indicators"
    assert INDICATOR_COLLECTION.startswith("ibkr")


def test_the_database_default_is_the_one_the_project_configures(monkeypatch):
    for key in ("MONGO_URI", "MONGO_USER", "MONGO_PASSWORD", "MONGO_DB",
                "MONGO_HOST", "MONGO_PORT"):
        monkeypatch.delenv(key, raising=False)
    assert build_uri() == "mongodb://localhost:27017/"


def test_one_client_serves_every_write(monkeypatch):
    """A full load calls get_database once per batch, tens of times. Each call
    building its own client leaks connection pools, and under MONGO_SSH_TUNNEL
    its own SSH tunnel with it."""
    import pipeline.mongo_sink as sink

    sink._CLIENTS.clear()
    for key in ("MONGO_URI", "MONGO_SSH_TUNNEL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MONGO_HOST", "db.internal")

    made = []

    class FakeClient(dict):
        def __init__(self, uri, **kwargs):
            made.append(uri)
            super().__init__()

        def __getitem__(self, name):
            return name

    monkeypatch.setitem(
        __import__("sys").modules, "pymongo",
        type("m", (), {"MongoClient": FakeClient})(),
    )
    try:
        for _ in range(5):
            sink.get_database()
        assert len(made) == 1, f"built {len(made)} clients for the same settings"

        monkeypatch.setenv("MONGO_HOST", "somewhere.else")
        sink.get_database()
        assert len(made) == 2, "changing the settings must give a new client"
    finally:
        sink._CLIENTS.clear()


def test_uri_is_built_from_parts_and_escapes_credentials(monkeypatch):
    for key in ("MONGO_URI", "MONGO_USER", "MONGO_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MONGO_HOST", "db.internal")
    monkeypatch.setenv("MONGO_PORT", "27018")
    assert build_uri() == "mongodb://db.internal:27018/"

    monkeypatch.setenv("MONGO_USER", "svc")
    monkeypatch.setenv("MONGO_PASSWORD", "p@ss:word")
    uri = build_uri()
    assert "p%40ss%3Aword" in uri, "an unescaped password breaks the URI"
    assert "authSource=admin" in uri


def test_an_explicit_host_overrides_the_uri_so_a_tunnel_can_win(monkeypatch):
    monkeypatch.setenv("MONGO_URI", "mongodb://db.internal:27017/")
    monkeypatch.delenv("MONGO_USER", raising=False)
    assert build_uri("127.0.0.1", 55001) == "mongodb://127.0.0.1:55001/"


def test_the_conflict_key_is_the_same_in_all_three_places():
    """The delivery rests on one claim: every write is keyed and idempotent, on
    both stores. That key is written down three times -- the PRIMARY KEY in the
    DDL an operator runs, the ON CONFLICT clause in the SQL sink, and the index
    tuple in the Mongo sink. Each is tested on its own, so one could be changed
    without the others and both suites would stay green, leaving one store
    silently duplicating rows.
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    ddl = (root / "pipeline" / "db" / "init_postgres" / "ibkr_tables.sql").read_text(
        encoding="utf-8"
    )
    sink = (root / "pipeline" / "db_sink.py").read_text(encoding="utf-8")

    def keys(text, pattern):
        return {
            tuple(c.strip() for c in match.split(","))
            for match in re.findall(pattern, text)
        }

    declared = keys(ddl, r"PRIMARY KEY \(([^)]*)\)")
    conflicts = keys(sink, r"ON CONFLICT \(([^)]*)\)")

    # The derived sinks build one statement with {key} substituted, so the
    # literal in the source carries the placeholder rather than the column.
    for name, key in (("indicator_name", INDICATOR_KEY), ("alpha_id", ALPHA_KEY)):
        assert key in declared, f"{name} key is not the DDL primary key"
        assert tuple(c.replace("{key}", name) for c in next(
            c for c in conflicts if "{key}" in c
        )) == key, f"{name} key is not the ON CONFLICT target"

    assert QUOTE_KEY in declared, "quote key is not the DDL primary key"
    assert QUOTE_KEY in conflicts, "quote key is not the ON CONFLICT target"
    # price_data is the platform's own table, so only the sink declares it here.
    assert PRICE_KEY in conflicts, "price key is not the ON CONFLICT target"
