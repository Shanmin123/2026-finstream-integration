"""
MongoDB writer for fundamentals output.
Author: Sarah Sodagudi

Writes to MongoDB {mongo.database}.{mongo.annual_collection} /
{mongo.quarter_collection}. Idempotent by design: each period is upserted
on (ticker, year, report_type) for annual records and
(ticker, year, quarter, report_type) for quarterly records -- re-running
the pipeline after a partial failure re-fetches everything but only ever
upserts, never creates a duplicate document. Including `ticker` in the
filter is deliberate: without it, two different companies' records for
the same year/report_type would silently overwrite each other.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List

from pymongo import MongoClient

logger = logging.getLogger(__name__)


def get_mongo_collections(config: dict):
    mongo_cfg = config["mongo"]
    client = MongoClient(host=mongo_cfg["host"], port=mongo_cfg["port"])
    db = client[mongo_cfg["database"]]
    return client, db[mongo_cfg["annual_collection"]], db[mongo_cfg["quarter_collection"]]


def _build_payload(ticker: str, exchange: str, period: Dict) -> Dict:
    return {
        "ticker": ticker,
        "exchange": exchange,
        "year": period["year"],
        "quarter": period["quarter"],
        "publish_date": period["publish_date"],
        "report_type": period["report_type"],
        "metrics": period["metrics"],
        "metadata": {
            "source": "eodhd",
            "processed_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def push_ticker_periods(ticker: str, exchange: str, periods: Dict[str, List[Dict]], config: dict) -> Dict:
    """
    periods: {"annual": [...], "quarterly": [...]} as returned by
    fundamentals_mapper.extract_all_periods.

    Returns a per-ticker summary: {"annual_pushed", "quarterly_pushed"}.
    Never raises for an individual document failure -- logs and continues,
    so one bad period can't abort the rest of the ticker's push.
    """
    client, annual_col, quarter_col = get_mongo_collections(config)
    summary = {"ticker": ticker, "annual_pushed": 0, "quarterly_pushed": 0}

    try:
        for period in periods.get("annual", []):
            payload = _build_payload(ticker, exchange, period)
            try:
                annual_col.update_one(
                    {"ticker": ticker, "year": payload["year"], "report_type": payload["report_type"]},
                    {"$set": payload},
                    upsert=True,
                )
                summary["annual_pushed"] += 1
            except Exception as exc:
                logger.error("Failed to push annual fundamentals for %s FY%s: %s", ticker, period["year"], exc)

        for period in periods.get("quarterly", []):
            payload = _build_payload(ticker, exchange, period)
            try:
                quarter_col.update_one(
                    {"ticker": ticker, "year": payload["year"], "quarter": payload["quarter"], "report_type": payload["report_type"]},
                    {"$set": payload},
                    upsert=True,
                )
                summary["quarterly_pushed"] += 1
            except Exception as exc:
                logger.error("Failed to push quarterly fundamentals for %s %s Q%s: %s", ticker, period["year"], period["quarter"], exc)
    finally:
        client.close()

    logger.info("%s: pushed %d annual + %d quarterly fundamental records.", ticker, summary["annual_pushed"], summary["quarterly_pushed"])
    return summary
