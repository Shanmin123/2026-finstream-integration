"""Optional MongoDB sink for the FinStream platform database.

The platform stores its shared datasets in MongoDB, so this module is the
MongoDB counterpart of :mod:`pipeline.db_sink`. It writes the same four
datasets, under the same field names, with the same conflict semantics:

    price_data            daily bars, one document per (ticker, day, interval)
    quote_ticks           streamed ticks, one per (ticker, ms, field, source)
    technical_indicators  long layout, one per (ticker, ms, name, interval, source)
    alpha_factors         long layout, one per (ticker, ms, alpha_id, interval, source)

Field names and conflict keys come from db/init_postgres/ibkr_tables.sql and
from the platform's own price_data contract.

Every document here is built by zipping a field-name tuple onto the row tuples
:mod:`pipeline.db_sink` already produces, so one mapper feeds both stores and a
length mismatch raises.

Conflict policy, mirroring the SQL:

  * price_data and quote_ticks are immutable observations. An existing document
    wins, and the write reports how many were actually inserted.
  * technical_indicators and alpha_factors are recomputable. A newer
    computation replaces an older one, and a provisional intraday value never
    replaces a final end-of-day value.

pymongo is an optional dependency. The parquet datasets stay the primary output
and the pipeline runs without any database.

Connection settings come from the environment, and the defaults match what the
other submissions configure: localhost:27017, database financial_db, no
authentication. Override with either a full URI::

    MONGO_URI=mongodb://user:pass@host:27017/?authSource=admin
    MONGO_DB=financial_db

or the parts::

    MONGO_HOST, MONGO_PORT, MONGO_USER, MONGO_PASSWORD, MONGO_DB, MONGO_AUTH_SOURCE

When MongoDB is only reachable through the bastion the platform already uses,
set MONGO_SSH_TUNNEL=1 and the sink opens the same tunnel utils/mongodb.py
opens, reading the platform's own variable names (host, user, password,
ssh_port) plus MONGO_REMOTE_HOST and MONGO_REMOTE_PORT for the database itself.

Collection names are overridable; see the constants below.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from pipeline.db_sink import (
    DAILY_INTERVAL,
    SOURCE,
    derived_frame_to_platform,
    price_rows_to_platform,
    quote_rows_to_platform,
)

logger = logging.getLogger("ibkr_pipeline")

# Collection names, each overridable by the matching environment variable.
#
# technical_indicators is taken: the fundamentals submission writes EODHD
# indicators there in a wide layout keyed on (ticker, timestamp_ms, interval),
# which has no source in it, so an IBKR value for a ticker-day would overwrite
# theirs. The default here is source-scoped. See README, "One collection name
# collides".
PRICE_COLLECTION = os.environ.get("MONGO_PRICE_COLLECTION", "price_data")
QUOTE_COLLECTION = os.environ.get("MONGO_QUOTE_COLLECTION", "quote_ticks")
INDICATOR_COLLECTION = os.environ.get(
    "MONGO_INDICATOR_COLLECTION", "ibkr_technical_indicators"
)
ALPHA_COLLECTION = os.environ.get("MONGO_ALPHA_COLLECTION", "alpha_factors")
RUN_COLLECTION = os.environ.get("MONGO_RUN_COLLECTION", "pipeline_runs")
COMPANY_COLLECTION = os.environ.get("MONGO_COMPANY_COLLECTION", "companies")

# Field order matches the tuples db_sink builds, which in turn match the SQL
# column order. Zipping the two is what keeps the stores identical.
PRICE_FIELDS = (
    "ticker", "timestamp_ms", "datetime_utc", "open", "high", "low",
    "close", "volume", "interval", "source",
)
QUOTE_FIELDS = (
    "ticker", "timestamp_ms", "datetime_utc", "field", "value",
    "market_data_type", "source",
)
# <key> is indicator_name for indicators and alpha_id for alphas.
DERIVED_FIELDS = (
    "ticker", "timestamp_ms", "datetime_utc", "<key>", "value", "interval",
    "is_provisional", "computed_at_utc", "source",
)

PRICE_KEY = ("ticker", "timestamp_ms", "interval")
QUOTE_KEY = ("ticker", "timestamp_ms", "field", "source")
INDICATOR_KEY = ("ticker", "timestamp_ms", "indicator_name", "interval", "source")
ALPHA_KEY = ("ticker", "timestamp_ms", "alpha_id", "interval", "source")


class FieldContractError(RuntimeError):
    """A db_sink mapper changed shape without this module following it."""


def _as_document(fields, values):
    if len(fields) != len(values):
        raise FieldContractError(
            f"expected {len(fields)} values for {fields}, got {len(values)}"
        )
    return dict(zip(fields, values))


def _as_datetime(value):
    """ISO strings are what the SQL sink emits; BSON wants a real datetime."""
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Row -> document
# ---------------------------------------------------------------------------

def price_documents(rows):
    """Daily bars as documents, from the same tuples the SQL sink writes."""
    documents = []
    for values in price_rows_to_platform(rows):
        document = _as_document(PRICE_FIELDS, values)
        document["datetime_utc"] = _as_datetime(document["datetime_utc"])
        documents.append(document)
    return documents


def quote_documents(rows, market_data_type=3):
    documents = []
    for values in quote_rows_to_platform(rows, market_data_type):
        document = _as_document(QUOTE_FIELDS, values)
        document["datetime_utc"] = _as_datetime(document["datetime_utc"])
        documents.append(document)
    return documents


def derived_documents(frame, key_column, interval=DAILY_INTERVAL):
    """Indicator or alpha rows as documents. ``key_column`` names the long key."""
    fields = tuple(key_column if f == "<key>" else f for f in DERIVED_FIELDS)
    documents = []
    for values in derived_frame_to_platform(frame, interval):
        document = _as_document(fields, values)
        document["datetime_utc"] = _as_datetime(document["datetime_utc"])
        document["computed_at_utc"] = _as_datetime(document["computed_at_utc"])
        # NaN warm-up cells and infinities are dropped by
        # derived_frame_to_platform, upstream of here.
        documents.append(document)
    return documents


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

# One tunnel and one client per distinct set of connection settings. Every
# write asks for a database, and a full load makes tens of those calls; without
# this each one would open its own client, and under MONGO_SSH_TUNNEL its own
# SSH tunnel, none of them closed.
_TUNNELS = {}
_CLIENTS = {}


def _tunnel_settings():
    return (
        os.environ["host"], int(os.environ.get("ssh_port", 22)),
        os.environ["user"], os.environ["password"],
        os.environ.get("MONGO_REMOTE_HOST", "127.0.0.1"),
        int(os.environ.get("MONGO_REMOTE_PORT", 27017)),
        int(os.environ.get("local_port", 0)),
    )


def _open_tunnel():
    """The bastion tunnel the platform's own client opens, for the database
    port rather than the API port. Returns (host, port)."""
    from sshtunnel import SSHTunnelForwarder

    key = _tunnel_settings()
    tunnel = _TUNNELS.get(key)
    if tunnel is None or not tunnel.is_active:
        host, ssh_port, user, password, remote_host, remote_port, local_port = key
        tunnel = SSHTunnelForwarder(
            (host, ssh_port),
            ssh_username=user,
            ssh_password=password,
            remote_bind_address=(remote_host, remote_port),
            local_bind_address=("127.0.0.1", local_port),
        )
        tunnel.start()
        _TUNNELS[key] = tunnel
    return "127.0.0.1", tunnel.local_bind_port


def close_connections():
    """Drop the cached client and tunnel. For tests and long-lived processes;
    a one-shot load can let the interpreter do it."""
    for client in _CLIENTS.values():
        try:
            client.close()
        except Exception:
            logger.warning("client_close_failed", exc_info=True)
    _CLIENTS.clear()
    for tunnel in _TUNNELS.values():
        try:
            tunnel.stop()
        except Exception:
            logger.warning("tunnel_stop_failed", exc_info=True)
    _TUNNELS.clear()


def build_uri(host=None, port=None):
    uri = os.environ.get("MONGO_URI")
    if uri and host is None:
        return uri
    host = host or os.environ.get("MONGO_HOST", "localhost")
    port = int(port or os.environ.get("MONGO_PORT", 27017))
    user = os.environ.get("MONGO_USER")
    password = os.environ.get("MONGO_PASSWORD")
    auth_source = os.environ.get("MONGO_AUTH_SOURCE", "admin")
    if user:
        from urllib.parse import quote_plus

        credentials = f"{quote_plus(user)}:{quote_plus(password or '')}@"
        return f"mongodb://{credentials}{host}:{port}/?authSource={auth_source}"
    return f"mongodb://{host}:{port}/"


def get_database():
    """The platform database. Raises rather than returning a broken handle.

    The client is cached on its connection settings, so changing the
    environment gives a new one and repeated calls under the same settings
    share a connection pool.
    """
    import pymongo

    host = port = None
    if os.environ.get("MONGO_SSH_TUNNEL", "").strip().lower() in {"1", "true", "yes"}:
        host, port = _open_tunnel()
    uri = build_uri(host, port)
    timeout = int(os.environ.get("MONGO_TIMEOUT_MS", 15000))
    key = (uri, timeout)
    client = _CLIENTS.get(key)
    if client is None:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=timeout,
                                     tz_aware=True)
        _CLIENTS[key] = client
    # financial_db is the database the other submissions' configs name.
    return client[os.environ.get("MONGO_DB", "financial_db")]


def ensure_indexes(database_factory=get_database):
    """Create the unique indexes that make every write here idempotent.

    Safe to call repeatedly: MongoDB ignores an index that already exists with
    the same specification.
    """
    database = database_factory()
    created = {}
    for collection, key in (
        (PRICE_COLLECTION, PRICE_KEY),
        (QUOTE_COLLECTION, QUOTE_KEY),
        (INDICATOR_COLLECTION, INDICATOR_KEY),
        (ALPHA_COLLECTION, ALPHA_KEY),
    ):
        created[collection] = database[collection].create_index(
            [(field, 1) for field in key], unique=True, name=f"{collection}_key"
        )
        database[collection].create_index(
            [("ticker", 1), ("datetime_utc", -1)], name=f"{collection}_ticker_time"
        )
    logger.info("db_indexes_ready", extra={"dataset": "*", "symbol": "*", "rows": 0})
    return created


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def _bulk(collection, operations, database_factory):
    if not operations:
        return 0
    from pymongo.errors import BulkWriteError

    database = database_factory()
    try:
        result = database[collection].bulk_write(operations, ordered=False)
    except BulkWriteError as exc:
        # A duplicate-key rejection is the conflict policy working, not a
        # failure: report what did land and let anything else surface.
        details = exc.details or {}
        if any(e.get("code") != 11000 for e in details.get("writeErrors", [])):
            raise
        written = details.get("nUpserted", 0) + details.get("nModified", 0)
    else:
        written = result.upserted_count + result.modified_count
    logger.info(
        "db_write_complete",
        extra={"dataset": collection, "symbol": "*", "rows": written},
    )
    if written < len(operations):
        logger.info(
            "db_rows_already_present",
            extra={
                "dataset": collection,
                "symbol": "*",
                "rows": len(operations) - written,
            },
        )
    return written


def _insert_if_absent(documents, key):
    """Mirror of ON CONFLICT DO NOTHING: the document already there wins.

    Only the non-key fields are set. On an upsert MongoDB builds the new
    document from the filter's equality clauses first, so the key fields arrive
    that way; naming them again in the operator would be asking the server to
    write a path the filter has already fixed. This also matches
    _upsert_if_newer, which sets the mutable fields for the same reason.
    """
    from pymongo import UpdateOne

    operations = []
    for document in documents:
        mutable = {k: v for k, v in document.items() if k not in key}
        operations.append(
            UpdateOne(
                {field: document[field] for field in key},
                {"$setOnInsert": mutable},
                upsert=True,
            )
        )
    return operations


def _upsert_if_newer(documents, key):
    """Mirror of the derived tables' DO UPDATE ... WHERE clause.

    A newer computation replaces an older one, and a provisional value never
    replaces a final one. The guard has to run inside the update rather than
    around it, so this is an aggregation-pipeline update: a read-then-write
    would let two collectors interleave and lose the newer value.
    """
    from pymongo import UpdateOne

    operations = []
    for document in documents:
        mutable = {k: v for k, v in document.items() if k not in key}
        apply_when = {
            "$or": [
                {"$eq": [{"$type": "$computed_at_utc"}, "missing"]},
                {
                    "$and": [
                        {"$gte": [document["computed_at_utc"], "$computed_at_utc"]},
                        {
                            "$or": [
                                {"$eq": [document["is_provisional"], False]},
                                {"$eq": ["$is_provisional", True]},
                            ]
                        },
                    ]
                },
            ]
        }
        operations.append(
            UpdateOne(
                {field: document[field] for field in key},
                [
                    {
                        "$set": {
                            field: {"$cond": [apply_when, value, f"${field}"]}
                            for field, value in mutable.items()
                        }
                    }
                ],
                upsert=True,
            )
        )
    return operations


def write_prices(rows, database_factory=get_database):
    """Insert daily bars. Returns the documents actually written; bars the
    primary feed already covers are left alone and counted as present."""
    documents = price_documents(rows)
    return _bulk(
        PRICE_COLLECTION,
        _insert_if_absent(documents, PRICE_KEY),
        database_factory,
    )


def write_quotes(rows, market_data_type=3, database_factory=get_database):
    documents = quote_documents(rows, market_data_type)
    return _bulk(
        QUOTE_COLLECTION,
        _insert_if_absent(documents, QUOTE_KEY),
        database_factory,
    )


def write_indicators(frame, interval=DAILY_INTERVAL, database_factory=get_database):
    documents = derived_documents(frame, "indicator_name", interval)
    return _bulk(
        INDICATOR_COLLECTION,
        _upsert_if_newer(documents, INDICATOR_KEY),
        database_factory,
    )


def write_alphas(frame, interval=DAILY_INTERVAL, database_factory=get_database):
    documents = derived_documents(frame, "alpha_id", interval)
    return _bulk(
        ALPHA_COLLECTION,
        _upsert_if_newer(documents, ALPHA_KEY),
        database_factory,
    )


def record_run(dag_id, records, latency_seconds, status, error=None,
               database_factory=get_database):
    database = database_factory()
    now = datetime.now(timezone.utc)
    database[RUN_COLLECTION].insert_one(
        {
            "dag_id": dag_id,
            "start_time": now - timedelta(seconds=max(float(latency_seconds), 0.0)),
            "end_time": now,
            "records_ingested": records,
            "latency_seconds": latency_seconds,
            "status": status,
            "error_message": error,
            "source": SOURCE,
        }
    )


def get_active_tickers(database_factory=get_database):
    """Active tickers from the platform, so DAGs do not hardcode a universe.

    Returns None when the database is unreachable, which lets the caller fall
    back to the configured symbols file exactly as the SQL sink does.
    """
    try:
        database = database_factory()
        cursor = database[COMPANY_COLLECTION].find(
            {"is_active": True}, {"ticker": 1, "_id": 0}
        ).sort("ticker", 1)
        tickers = [row["ticker"] for row in cursor if row.get("ticker")]
    except Exception as exc:
        # An auth failure, bad DNS and a stopped database each need different
        # operator action, so the reason is logged rather than swallowed.
        logger.warning(
            "db_unavailable: %s", exc,
            extra={"dataset": COMPANY_COLLECTION, "symbol": "*", "rows": 0},
        )
        return None
    return tickers or None
