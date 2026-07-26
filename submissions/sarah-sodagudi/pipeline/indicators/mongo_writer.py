"""
MongoDB writer for technical indicator output.
Author: Sarah Sodagudi

Writes to MongoDB {mongo.database}.{mongo.collection} (default
technical_indicators). Idempotency: bulk upsert keyed on
(ticker, timestamp_ms, interval) -- same dedup-key discipline as the
fundamentals pipeline. An UpdateOne upsert also REFRESHES an existing
document's fields if the same key is recomputed later (e.g. a vendor
indicator field added retroactively by widening vendor.functions) -- it
still never creates a duplicate document, just a more useful property
for a document store expected to accumulate wide, append-only fields
over time.

Resumability: this pipeline's resume mechanism is the MongoDB watermark
itself (get_last_processed_timestamp_ms), not a separate checkpoint
file -- price_data doesn't change once written, so an interrupted run
that never reached the final bulk write simply gets recomputed from
scratch on the next invocation at no cost beyond the wasted compute time
(no risk of double-counting, since the same watermark-filtered window
gets upserted, not appended).
"""
import logging
from datetime import datetime, timezone

import pandas as pd
from pymongo import MongoClient, UpdateOne

logger = logging.getLogger(__name__)


def _get_collection(config: dict):
    mongo_cfg = config["mongo"]
    client = MongoClient(host=mongo_cfg["host"], port=mongo_cfg["port"])
    db = client[mongo_cfg["database"]]
    return client, db[mongo_cfg["collection"]]


def get_last_processed_timestamp_ms(config: dict, interval: str) -> int:
    """Watermark for incremental processing: the newest timestamp_ms
    already stored for this interval. Returns 0 (process everything
    within the read-side lookback window, see price_reader.py) on the
    very first run, i.e. before any documents exist for this interval."""
    client, collection = _get_collection(config)
    try:
        latest = collection.find_one(
            {"interval": interval}, sort=[("timestamp_ms", -1)], projection={"timestamp_ms": 1}
        )
        return int(latest["timestamp_ms"]) if latest else 0
    finally:
        client.close()


def push_indicators_to_mongo(config: dict, pdf: pd.DataFrame) -> int:
    """
    Bulk-upsert indicator rows into MongoDB. `pdf` must contain ticker,
    timestamp_ms, datetime_utc, interval. Returns the number of documents
    newly inserted or updated.
    """
    if pdf is None or pdf.empty:
        logger.info("No records to push. DataFrame is empty.")
        return 0

    required = ("ticker", "timestamp_ms", "datetime_utc", "interval")
    missing = [c for c in required if c not in pdf.columns]
    if missing:
        raise ValueError(f"Cannot push indicators: missing required key column(s) {missing}")

    clean_pdf = pdf.where(pd.notnull(pdf), None)
    # BSON needs native python datetimes, not pandas.Timestamp.
    clean_pdf["datetime_utc"] = pd.to_datetime(clean_pdf["datetime_utc"]).dt.to_pydatetime()

    records = clean_pdf.to_dict("records")
    for record in records:
        record.setdefault("metadata", {})
        record["metadata"] = {"source": "eodhd", "processed_at": datetime.now(timezone.utc).isoformat()}

    operations = []
    for record in records:
        filter_query = {
            "ticker": record["ticker"],
            "timestamp_ms": int(record["timestamp_ms"]),
            "interval": record["interval"],
        }
        operations.append(UpdateOne(filter_query, {"$set": record}, upsert=True))

    client, collection = _get_collection(config)
    try:
        result = collection.bulk_write(operations, ordered=False)
        written = result.upserted_count + result.modified_count
        logger.info(
            "Upserted indicators: %d new, %d updated (%d rows attempted).",
            result.upserted_count, result.modified_count, len(records),
        )
        return written
    except Exception:
        logger.exception("Failed to push indicators to MongoDB")
        raise
    finally:
        client.close()
