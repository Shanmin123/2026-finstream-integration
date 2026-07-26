"""
Reader for the shared price_data table (read-only).
Author: Sarah Sodagudi

Data source  : PostgreSQL `price_data` table, populated by the platform's
               own price-ingestion pipeline. Schema: ticker, timestamp_ms
               (bigint, epoch ms), datetime_utc, open/high/low/close,
               volume, interval ('1d' or '5m'), source. Read-only --
               this module only ever runs SELECT statements.
Known quirk  : a small fraction of intraday (5m) bars can have a NULL
               volume on the final bar of the trading day (an EOD
               consolidation artefact on the source side). This reader
               filters `WHERE volume IS NOT NULL` before returning any
               rows, so every volume-based indicator computed downstream
               (OBV, Alpha#6, Alpha#12, CMF) never sees a NULL volume row.
"""
import logging
from typing import List

import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)


def _connect(config: dict):
    pg_cfg = config["postgres"]
    return psycopg2.connect(
        host=pg_cfg["host"], port=pg_cfg["port"], user=pg_cfg["user"],
        password=pg_cfg["password"], dbname=pg_cfg["database"],
    )


def get_latest_price_timestamp_ms(config: dict, interval: str) -> int:
    """Read-only MAX(timestamp_ms) currently in price_data for `interval`."""
    conn = _connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COALESCE(MAX(timestamp_ms), 0) FROM {config['postgres']['price_table']} WHERE interval = %s",
                (interval,),
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
    finally:
        conn.close()


def read_recent_price_data(config: dict, interval: str, active_tickers_only: bool = True) -> pd.DataFrame:
    """
    Read a trailing lookback window of price_data for `interval`, with
    NULL-volume rows already filtered out, and (by default) restricted to
    tickers currently active in the shared `companies` table -- so
    indicators aren't computed/stored for delisted/removed constituents
    that might still have historical rows sitting in price_data. The
    lookback window is wider than "just the new bars" on purpose --
    rolling indicators (SMA_50 etc.) need trailing history to compute
    correctly for the newest bars; main.py is responsible for only
    pushing rows newer than the MongoDB watermark after computation.

    If the companies table has zero active rows yet, the active-ticker
    filter is automatically skipped (with a warning) rather than silently
    returning zero rows.

    Uses a raw psycopg2 connection rather than a SQLAlchemy Engine
    deliberately: pandas 1.3.5's `pd.read_sql(query, engine)` internally
    calls a SQLAlchemy 1.x-only positional signature that SQLAlchemy 2.x
    removed -- on any environment where SQLAlchemy resolves to 2.x this
    raises a TypeError. A raw DBAPI connection sidesteps pandas' engine-
    specific code path entirely, so this works regardless of which
    SQLAlchemy version happens to be installed.
    """
    lookback_days = config["lookback_days"].get(interval, 5)
    pg_cfg = config["postgres"]

    conn = _connect(config)
    try:
        ticker_filter_clause = ""
        if active_tickers_only:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {pg_cfg['companies_table']} WHERE is_active = TRUE")
                active_count = cur.fetchone()[0]
            if active_count > 0:
                ticker_filter_clause = f"AND ticker IN (SELECT ticker FROM {pg_cfg['companies_table']} WHERE is_active = TRUE)"
            else:
                logger.warning(
                    "%s has no active tickers yet -- processing ALL tickers present in price_data for interval=%s.",
                    pg_cfg["companies_table"], interval,
                )

        query = f"""
            SELECT ticker, timestamp_ms, datetime_utc, open, high, low, close, volume, interval
            FROM {pg_cfg['price_table']}
            WHERE interval = %(interval)s
              AND volume IS NOT NULL
              AND datetime_utc >= NOW() - (%(lookback_days)s || ' days')::interval
              {ticker_filter_clause}
            ORDER BY ticker, timestamp_ms
        """
        df = pd.read_sql(query, conn, params={"interval": interval, "lookback_days": str(lookback_days)})
    finally:
        conn.close()

    logger.info(
        "Read %d price_data rows for interval=%s (lookback=%dd, volume-filtered, active_only=%s)",
        len(df), interval, lookback_days, bool(ticker_filter_clause),
    )
    return df
