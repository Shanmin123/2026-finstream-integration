"""Optional PostgreSQL sink for the FinStream platform database.

Maps this pipeline's rows onto the platform's existing `price_data` contract
(ticker, timestamp_ms, datetime_utc, open, high, low, close, volume, interval,
source) with the same idempotent conflict key (ticker, timestamp_ms, interval),
and records each run in `pipeline_runs`. Rows carry source='ibkr' so they are
distinguishable from the EODHD feed.

psycopg2 is an optional dependency: the parquet datasets remain the primary
output and the pipeline runs without a database. Connection settings come from
the platform's environment variables (POSTGRES_HOST/PORT/USER/PASSWORD/DB).

Only daily bars have an agreed platform table. Tables for streamed quotes and
for the derived indicator/alpha datasets are still open with the integration
team, so those datasets stay in parquet until a schema is confirmed.
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
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO price_data
                    (ticker, timestamp_ms, datetime_utc, open, high, low,
                     close, volume, interval, source)
                VALUES %s
                ON CONFLICT (ticker, timestamp_ms, interval) DO NOTHING
                """,
                mapped,
            )
        conn.commit()
    finally:
        conn.close()
    logger.info(
        "db_write_complete",
        extra={"dataset": "price_data", "symbol": "*", "rows": len(mapped)},
    )
    return len(mapped)


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
