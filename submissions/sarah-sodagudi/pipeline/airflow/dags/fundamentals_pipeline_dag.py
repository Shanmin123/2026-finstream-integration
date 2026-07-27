"""
Airflow DAG: EODHD Fundamentals Pipeline
Author: Sarah Sodagudi

Thin Airflow wrapper around pipeline/main.py's collect -> process -> save
flow. Deliberately kept as a SINGLE task rather than three tasks
connected by XCom: passing the full per-ticker raw EODHD JSON payload
(hundreds of KB across ~500 tickers) through Airflow's XCom -- which is
backed by the metadata database, not meant for bulk data -- is a known
anti-pattern. It's exactly what caused an OOM on a prior full-S&P-500
run of a similarly-shaped price DAG in this same integration effort
(see eodhd_price_dag.py's docstring in submissions/emaad-ahmad/ for the
prior incident this is deliberately avoiding). Collect/process/save stay
separate FUNCTIONS (for testability and the template's required shape,
see pipeline/main.py) but run inside one Airflow task/process.

Schedule: manual/triggered (schedule=None) -- production cadence is a
platform-level decision (Sean), not made here. Runs are idempotent
(Mongo upserts keyed on (ticker, year, [quarter,] report_type)) and
resumable (checkpoint saved after every ticker, not just at the end) --
see pipeline/main.py and README.md's "Resuming after interruption".
"""
import logging
import os
import sys

import pendulum
from airflow import DAG
from airflow.decorators import task

# pipeline/main.py and its sibling modules (eodhd_client.py,
# fundamentals_mapper.py, ticker_universe.py, mongo_writer.py) are mounted
# at PIPELINE_SRC by docker-compose.yml -- see that file's `volumes:`.
# Airflow only puts the dags/ folder itself on sys.path, not siblings, so
# this needs adding explicitly (same fix needed in airflow/sarah-docker/
# elsewhere in this project, for the same underlying reason).
PIPELINE_SRC = os.environ.get("PIPELINE_SRC", "/opt/airflow/pipeline_src")
sys.path.insert(0, PIPELINE_SRC)
os.chdir(PIPELINE_SRC)  # so main.py's default relative "config.yaml" / checkpoint paths resolve correctly

from main import collect, load_checkpoint, load_config, process, save, save_checkpoint  # noqa: E402

logger = logging.getLogger(__name__)

with DAG(
    dag_id="sarah_fundamentals_pipeline",
    schedule=None,  # manual/triggered -- production cadence pending Sean's confirmation
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["fundamentals", "eodhd", "sarah"],
    doc_md="""
    ### Sarah's EODHD Fundamentals Pipeline
    Fetches annual + quarterly fundamentals for every active S&P 500
    constituent from EODHD and upserts them into MongoDB
    (`annual_fundamental` / `quarter_fundamental`). See
    `submissions/sarah-sodagudi/README.md` for full setup and
    `metadata.yaml` for data source details.
    """,
) as dag:

    @task(task_id="run_fundamentals_pipeline")
    def run_fundamentals_pipeline():
        config = load_config()
        checkpoint = load_checkpoint(config["paths"]["checkpoint_file"])

        try:
            raw_data = collect(config, checkpoint)
            processed_data = process(raw_data)
            totals = save(processed_data, config)
        except Exception:
            logger.exception("Pipeline failed -- checkpoint preserved for resume")
            raise
        else:
            save_checkpoint(config["paths"]["checkpoint_file"], {"completed_tickers": []})
            logger.info(f"Pipeline completed successfully: {totals}")
            return totals

    run_fundamentals_pipeline()
