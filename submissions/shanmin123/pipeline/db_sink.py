"""Optional PostgreSQL sink for the FinStream platform database.

Maps this pipeline's rows onto the platform's existing `price_data` contract
(ticker, timestamp_ms, datetime_utc, open, high, low, close, volume, interval,
source) and records each run in `pipeline_runs`.

Note on price_data: the platform's constraint is UNIQUE(ticker, timestamp_ms,
interval) and does NOT include source, so an IBKR bar cannot coexist with an
EODHD bar for the same ticker/day. With the platform's DO NOTHING policy the
existing row wins, which makes this feed a GAP FILLER: it supplies bars for
tickers or days the primary feed has not covered, and is a no-op elsewhere. The
write reports how many rows were actually inserted so the difference is visible
rather than silent. Making both sources coexist would need the platform owner to
add source to that constraint.

psycopg2 is an optional dependency: the parquet datasets remain the primary
output and the pipeline runs without a database. Connection settings come from
the platform's environment variables (POSTGRES_HOST/PORT/USER/PASSWORD/DB).

Daily bars go to the platform's own price_data table. Streamed quotes and the
derived indicator/alpha datasets get three tables defined in
db/init_postgres/ibkr_tables.sql, which follow the same column naming and use
the long (name, value) layout this project's collectors already use, so new
indicators or alpha formulas need no migration.
"""

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger("ibkr_pipeline")

SOURCE = "ibkr"
DAILY_INTERVAL = "1d"


def _epoch_ms(event_date):
    moment = datetime.strptime(str(event_date)[:10], "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    return int(moment.timestamp() * 1000), moment.isoformat()


def price_rows_to_platform(rows):
    """Map collector price rows onto the platform's price_data tuple order."""
    mapped = []
    for row in rows:
        timestamp_ms, datetime_utc = _epoch_ms(row["event_date"])
        mapped.append(
            (
                row["symbol"],
                timestamp_ms,
                datetime_utc,
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
                DAILY_INTERVAL,
                SOURCE,
            )
        )
    return mapped


def get_connection():
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ.get("POSTGRES_USER", "finplatform"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        dbname=os.environ.get("POSTGRES_DB", "financial_data"),
    )


def write_prices(rows, connection_factory=get_connection):
    """Upsert daily bars into price_data. Returns the number of rows sent."""
    mapped = price_rows_to_platform(rows)
    if not mapped:
        return 0
    from psycopg2.extras import execute_values

    conn = connection_factory()
    inserted = 0
    try:
        with conn.cursor() as cur:
            # execute_values pages the batch and psycopg2 documents that
            # cursor.rowcount does NOT hold the total afterwards, so the count
            # comes from RETURNING rows gathered across every page.
            returned = execute_values(
                cur,
                """
                INSERT INTO price_data
                    (ticker, timestamp_ms, datetime_utc, open, high, low,
                     close, volume, interval, source)
                VALUES %s
                ON CONFLICT (ticker, timestamp_ms, interval) DO NOTHING
                RETURNING 1
                """,
                mapped,
                fetch=True,
            )
            inserted = len(returned or [])
        conn.commit()
    finally:
        conn.close()
    logger.info(
        "db_write_complete",
        extra={"dataset": "price_data", "symbol": "*", "rows": inserted},
    )
    if inserted < len(mapped):
        logger.info(
            "db_rows_already_present",
            extra={
                "dataset": "price_data",
                "symbol": "*",
                "rows": len(mapped) - inserted,
            },
        )
    return inserted


def record_run(dag_id, records, latency_seconds, status, error=None,
               connection_factory=get_connection):
    conn = connection_factory()
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (dag_id, start_time, end_time, records_ingested,
                     latency_seconds, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (dag_id, now, now, records, latency_seconds, status, error),
            )
        conn.commit()
    finally:
        conn.close()


def get_active_tickers(connection_factory=get_connection):
    """Active S&P 500 tickers from the platform, so DAGs do not hardcode lists.

    Returns None when the platform database is unreachable, letting the caller
    fall back to the configured symbols file.
    """
    try:
        conn = connection_factory()
    except Exception as exc:
        logger.warning("db_unavailable", extra={"dataset": "companies",
                                                "symbol": "*", "rows": 0})
        logger.debug("db_unavailable_detail: %s", exc)
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker FROM companies WHERE is_active = TRUE ORDER BY ticker"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Streamed quotes and the derived long-format datasets (see
# db/init_postgres/ibkr_tables.sql for the DDL these match).
# ---------------------------------------------------------------------------

_NON_VALUE_COLUMNS = {
    "symbol", "event_date", "computed_at_utc", "as_of_utc", "provisional",
}


def _epoch_ms_from_iso(value):
    text = str(value).replace("Z", "+00:00")
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    moment = moment.astimezone(timezone.utc)
    return int(moment.timestamp() * 1000), moment.isoformat()


def quote_rows_to_platform(rows, market_data_type=3):
    mapped = []
    for row in rows:
        timestamp_ms, datetime_utc = _epoch_ms_from_iso(row["tick_time_utc"])
        mapped.append(
            (
                row["symbol"],
                timestamp_ms,
                datetime_utc,
                row["field"],
                row.get("value"),
                market_data_type,
                SOURCE,
            )
        )
    return mapped


def derived_frame_to_platform(frame, interval=DAILY_INTERVAL):
    """Melt a wide indicator/alpha frame into the platform's long rows."""
    if frame is None or frame.empty:
        return []
    provisional = bool(frame["provisional"].iloc[0]) if "provisional" in frame else False
    stamp_column = "as_of_utc" if "as_of_utc" in frame else "computed_at_utc"
    value_columns = [c for c in frame.columns if c not in _NON_VALUE_COLUMNS]
    mapped = []
    for record in frame.to_dict("records"):
        timestamp_ms, datetime_utc = _epoch_ms(record["event_date"])
        computed_at = record.get(stamp_column)
        for name in value_columns:
            value = record.get(name)
            if value is None or value != value:      # skip NaN warm-up cells
                continue
            mapped.append(
                (
                    record["symbol"],
                    timestamp_ms,
                    datetime_utc,
                    name,
                    float(value),
                    interval,
                    provisional,
                    computed_at,
                    SOURCE,
                )
            )
    return mapped


def _execute(sql, mapped, connection_factory):
    if not mapped:
        return 0
    from psycopg2.extras import execute_values

    conn = connection_factory()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, mapped)
        conn.commit()
    finally:
        conn.close()
    return len(mapped)


def write_quotes(rows, market_data_type=3, connection_factory=get_connection):
    """Insert streamed ticks. Ticks are immutable observations, so conflicts are
    ignored exactly as the platform does for price_data."""
    sent = _execute(
        """
        INSERT INTO quote_ticks
            (ticker, timestamp_ms, datetime_utc, field, value,
             market_data_type, source)
        VALUES %s
        ON CONFLICT (ticker, timestamp_ms, field, source) DO NOTHING
        """,
        quote_rows_to_platform(rows, market_data_type),
        connection_factory,
    )
    if sent:
        logger.info(
            "db_write_complete",
            extra={"dataset": "quote_ticks", "symbol": "*", "rows": sent},
        )
    return sent


def _write_derived(table, key_column, frame, interval, connection_factory):
    sql = """
        INSERT INTO {table}
            (ticker, timestamp_ms, datetime_utc, {key}, value, interval,
             is_provisional, computed_at_utc, source)
        VALUES %s
        ON CONFLICT (ticker, timestamp_ms, {key}, interval, source)
        DO UPDATE SET
            value = EXCLUDED.value,
            is_provisional = EXCLUDED.is_provisional,
            computed_at_utc = EXCLUDED.computed_at_utc
        WHERE EXCLUDED.computed_at_utc >= {table}.computed_at_utc
          AND (EXCLUDED.is_provisional = FALSE OR {table}.is_provisional = TRUE)
    """.format(table=table, key=key_column)
    sent = _execute(sql, derived_frame_to_platform(frame, interval), connection_factory)
    if sent:
        logger.info(
            "db_write_complete", extra={"dataset": table, "symbol": "*", "rows": sent}
        )
    return sent


def write_indicators(frame, interval=DAILY_INTERVAL, connection_factory=get_connection):
    """Upsert indicator values. Newer computations win, so a provisional
    intraday row is replaced by the final end-of-day value in place."""
    return _write_derived(
        "technical_indicators", "indicator_name", frame, interval, connection_factory
    )


def write_alphas(frame, interval=DAILY_INTERVAL, connection_factory=get_connection):
    return _write_derived(
        "alpha_factors", "alpha_id", frame, interval, connection_factory
    )
