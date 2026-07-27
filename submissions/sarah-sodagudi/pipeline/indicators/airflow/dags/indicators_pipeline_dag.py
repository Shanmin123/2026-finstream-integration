"""
Airflow DAG: Technical + Alpha101 Indicators Pipeline
Author: Sarah Sodagudi

Thin Airflow wrapper around pipeline/indicators/main.py's
read -> compute -> vendor-enrich -> push flow, kept as a SINGLE task
(same anti-XCom-bulk-data reasoning as the fundamentals DAG -- passing
per-ticker OHLCV/indicator dataframes through Airflow's XCom, backed by
the metadata database rather than meant for bulk data, is a known
anti-pattern in this integration effort).

Schedule: manual/triggered (schedule=None) -- production cadence is a
platform-level decision, not made here. Intended to run downstream of
the platform's own price-ingestion DAGs; if those run in a separate
Airflow instance, poll price_data's own MAX(timestamp_ms) directly
(see price_reader.get_latest_price_timestamp_ms) rather than relying on
a same-instance sensor/TriggerDagRunOperator that can't reach across
independent Airflow deployments.

Runs are idempotent (MongoDB upserts keyed on (ticker, timestamp_ms,
interval)) and resumable via the MongoDB watermark itself, not a
separate checkpoint file -- see pipeline/indicators/mongo_writer.py.
"""
import logging
import os
import sys

import pendulum
from airflow import DAG
from airflow.decorators import task

# main.py and its sibling modules (price_reader.py, indicator_calculator.py,
# pandas_indicators.py, eodhd_technical_client.py, technical_comparison.py,
# mongo_writer.py) are mounted at PIPELINE_SRC by docker-compose.yml -- see
# that file's `volumes:`. Airflow only puts the dags/ folder itself on
# sys.path, not siblings, so this needs adding explicitly.
PIPELINE_SRC = os.environ.get("PIPELINE_SRC", "/opt/airflow/pipeline_src")
sys.path.insert(0, PIPELINE_SRC)
os.chdir(PIPELINE_SRC)  # so main.py's default relative "config.yaml" path resolves correctly

from main import load_config, run_interval  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

logger = logging.getLogger(__name__)

with DAG(
    dag_id="sarah_indicators_pipeline",
    schedule=None,  # manual/triggered -- production cadence pending confirmation
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["indicators", "alpha101", "sarah"],
    doc_md="""
    ### Sarah's Technical + Alpha101 Indicators Pipeline
    Reads price_data, computes technical + Alpha101-style indicators
    (Spark + pandas stages), additively cross-checks a validated subset
    against EODHD's Technical Indicator API, and upserts into MongoDB
    (`technical_indicators`). See `submissions/sarah-sodagudi/README.md`
    for full setup and `metadata.yaml` for data source details.
    """,
) as dag:

    @task(task_id="run_indicators_pipeline")
    def run_indicators_pipeline():
        config = load_config()
        spark = SparkSession.builder.appName("SarahIndicatorCalculator").master("local[*]").getOrCreate()
        totals = {}
        try:
            for interval in config["run"]["intervals"]:
                totals[interval] = run_interval(spark, config, interval)
        finally:
            spark.stop()
        logger.info("Indicators DAG run complete: %s", totals)
        return totals

    run_indicators_pipeline()
