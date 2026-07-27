"""
Technical + Alpha101 indicators pipeline: price_data -> MongoDB.

Follows the patterns required by the integration guidelines:
- config loaded from separate files, not hardcoded -- secrets in .env,
  everything else (hosts, ports, vendor scope, lookback windows) in
  config.yaml
- logging for diagnosability
- resumable after interruption (see mongo_writer.py's module docstring
  for why this pipeline's resume mechanism is the MongoDB watermark
  itself rather than a separate checkpoint file)

Two local compute stages (PySpark for rolling-window aggregates, pandas
for recursive/smoothed indicators Spark's windowed functions can't
express), then an additive EODHD vendor cross-check for a validated
subset of indicators -- see indicator_calculator.py, pandas_indicators.py,
technical_comparison.py for the calculation logic itself.
"""
import logging
import os

import yaml
from pyspark.sql import SparkSession

from indicator_calculator import process_price_stream
from mongo_writer import get_last_processed_timestamp_ms, push_indicators_to_mongo
from pandas_indicators import add_pandas_indicators
from price_reader import read_recent_price_data
from technical_comparison import enrich_with_vendor_indicators

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def load_env_file(path: str = ".env") -> None:
    """
    Minimal .env parser (KEY=VALUE per line, '#' comments and blank lines
    skipped) -- deliberately no python-dotenv dependency, same convention
    used by the fundamentals pipeline's main.py. Existing real environment
    variables always take precedence over the file (os.environ.setdefault),
    e.g. CI or a production scheduler injecting EODHD_API_TOKEN directly
    still wins over anything left in a local .env.
    """
    if not os.path.exists(path):
        return
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_config(path: str = "config.yaml", env_path: str = ".env") -> dict:
    """Loads config.yaml (non-secret settings) and merges in secrets from
    .env / the real environment (EODHD_API_TOKEN, POSTGRES_USER,
    POSTGRES_PASSWORD) -- see .env.example for what's expected.

    POSTGRES_HOST/MONGO_HOST (optional env vars) override config.yaml's
    postgres.host/mongo.host if set -- config.yaml's own values are
    correct for running `python main.py` directly on the host
    ("localhost" reaches Postgres/Mongo bound to the host machine), but
    wrong from inside the Airflow container, where "localhost" means the
    container itself, not the host. docker-compose.yml sets both to
    host.docker.internal for the containerised run so the SAME
    config.yaml works for both without editing it back and forth.
    """
    load_env_file(env_path)

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    config["api"]["api_key"] = os.environ.get("EODHD_API_TOKEN", "")
    config["postgres"]["user"] = os.environ.get("POSTGRES_USER", "")
    config["postgres"]["password"] = os.environ.get("POSTGRES_PASSWORD", "")
    config["postgres"]["host"] = os.environ.get("POSTGRES_HOST", config["postgres"]["host"])
    config["mongo"]["host"] = os.environ.get("MONGO_HOST", config["mongo"]["host"])

    return config


def run_interval(spark: SparkSession, config: dict, interval: str) -> int:
    """Reads, computes, cross-checks, and pushes new rows for one
    interval. Returns the number of documents written."""
    raw_pdf = read_recent_price_data(config, interval)
    if raw_pdf.empty:
        logger.info("No price_data rows for interval=%s; skipping.", interval)
        return 0

    watermark_ms = get_last_processed_timestamp_ms(config, interval)

    spark_df = spark.createDataFrame(raw_pdf)
    enriched_spark_df = process_price_stream(spark_df)
    enriched_pdf = enriched_spark_df.toPandas()

    # Pandas-stage recursive/smoothed indicators need the same trailing
    # history the Spark stage did -- compute over the WHOLE window,
    # filter to new rows only afterwards.
    enriched_pdf = add_pandas_indicators(enriched_pdf, interval)

    new_pdf = enriched_pdf[enriched_pdf["timestamp_ms"] > watermark_ms].copy()
    if new_pdf.empty:
        logger.info("No new bars past watermark %d for interval=%s.", watermark_ms, interval)
        return 0

    # Vendor pull only against the genuinely new rows -- avoids
    # re-spending EODHD credits on history already enriched.
    new_pdf = enrich_with_vendor_indicators(config, new_pdf, interval)

    written = push_indicators_to_mongo(config, new_pdf)
    logger.info("interval=%s: computed %d new rows, upserted %d", interval, len(new_pdf), written)
    return written


def main():
    config = load_config()

    spark = SparkSession.builder \
        .appName("SarahIndicatorCalculator") \
        .master("local[*]") \
        .getOrCreate()

    totals = {}
    try:
        for interval in config["run"]["intervals"]:
            totals[interval] = run_interval(spark, config, interval)
    finally:
        spark.stop()

    logger.info("Pipeline completed successfully: %s", totals)
    return totals


if __name__ == "__main__":
    main()
