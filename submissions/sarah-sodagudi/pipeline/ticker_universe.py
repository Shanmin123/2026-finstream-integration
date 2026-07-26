"""
S&P 500 ticker universe loader.
Author: Sarah Sodagudi

Primary source: the shared PostgreSQL `companies` table (READ-ONLY --
this module never writes to it), filtered `WHERE is_active = TRUE`. This
is the same single source of truth the price-ingestion pipeline maintains
(refreshed periodically from an authoritative constituents list), so
fundamentals collection stays aligned with whichever tickers are actually
being tracked elsewhere in the platform.

Fallback: a local CSV (config["paths"]["sp500_constituents_csv"]) if the
companies table is unreachable or has zero active rows -- e.g. before
that table has ever been refreshed. The CSV itself is NOT committed to
this repository (data policy, see CONTRIBUTING.md) -- point the config
path at wherever your own copy lives.

Both paths return [(ticker, exchange), ...]. Exchange is always "US" to
match what EODHD's endpoints expect (their `.US` suffix convention for
all US-listed tickers, regardless of the actual listing venue).
"""
import csv
import logging
import os
from typing import List, Tuple

import psycopg2

logger = logging.getLogger(__name__)

_DEFAULT_EXCHANGE = "US"
_TICKER_ALIASES = ("ticker", "symbol")
_EXCHANGE_ALIASES = ("exchange", "exch")


def get_active_companies(config: dict) -> List[Tuple[str, str]]:
    """Read-only SELECT against the companies table -- [(ticker, "US"), ...]
    for every currently active constituent."""
    pg_cfg = config["postgres"]
    conn = psycopg2.connect(
        host=pg_cfg["host"], port=pg_cfg["port"], user=pg_cfg["user"],
        password=pg_cfg["password"], dbname=pg_cfg["database"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT ticker FROM {pg_cfg['companies_table']} WHERE is_active = TRUE ORDER BY ticker")
            tickers = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

    logger.info("Loaded %d active tickers from %s.", len(tickers), pg_cfg["companies_table"])
    return [(ticker, _DEFAULT_EXCHANGE) for ticker in tickers]


def _find_column(fieldnames: List[str], aliases: Tuple[str, ...]):
    lowered = {name.lower().strip(): name for name in fieldnames}
    for alias in aliases:
        if alias in lowered:
            return lowered[alias]
    return None


def load_csv_universe(config: dict) -> List[Tuple[str, str]]:
    """Fallback ticker universe from a local CSV. Column names matched
    case-insensitively (ticker/symbol; exchange optional, defaults US)."""
    path = config["paths"]["sp500_constituents_csv"]
    if not os.path.exists(path):
        logger.error("Fallback constituents CSV not found at %s.", path)
        return []

    universe = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            logger.error("Constituents CSV at %s has no header row.", path)
            return []

        ticker_col = _find_column(reader.fieldnames, _TICKER_ALIASES)
        exchange_col = _find_column(reader.fieldnames, _EXCHANGE_ALIASES)
        if ticker_col is None:
            logger.error("Could not find a ticker/symbol column in %s (found: %s).", path, reader.fieldnames)
            return []

        for row in reader:
            ticker = (row.get(ticker_col) or "").strip().upper()
            if not ticker:
                continue
            exchange = ((row.get(exchange_col) or "").strip().upper() if exchange_col else "") or _DEFAULT_EXCHANGE
            universe.append((ticker, exchange))

    logger.info("Loaded %d tickers from fallback CSV (%s).", len(universe), path)
    return universe


def get_ticker_universe(config: dict) -> List[Tuple[str, str]]:
    """Prefer the companies table; fall back to the CSV if that read
    fails or comes back empty."""
    try:
        universe = get_active_companies(config)
        if universe:
            return universe
        logger.warning("companies table returned zero active tickers -- falling back to CSV.")
    except Exception as exc:
        logger.warning("companies table unreachable (%s) -- falling back to CSV.", exc)

    return load_csv_universe(config)
