"""
DAG: IBKR streaming quotes + live features
==========================================
Runs the continuous streaming pipeline as a bounded Airflow task: one long-lived
process per run that subscribes to IBKR quote ticks and, on every flush interval,
appends the ticks to the quotes dataset and recomputes the provisional intraday
indicator/alpha rows for the symbols that moved.

Data flow:
  IB Gateway (reqMktData tick stream) -> quotes parquet
                                      -> indicators_live / alphas_live parquet

Latency: the free paper tier streams delayed data (15-20 minutes behind). With
market-data subscriptions the same task streams real time by setting
`stream.market_data_type: 1` in the config — no code change.

The task is bounded by `stream.duration_seconds` so Airflow keeps a normal
task lifecycle; schedule it back-to-back across the session for continuous
coverage (e.g. "*/30 13-20 * * 1-5" with duration_seconds 1800).

Streamed quotes and the provisional live features have no agreed platform table
yet, so this DAG writes the parquet datasets only. The PostgreSQL sink is wired
for daily bars in `ibkr_prices_dag`; extending it needs a schema decision from
the integration team.

`schedule=None` during validation.
"""

from __future__ import annotations

import logging
import os

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("IBKR_PIPELINE_CONFIG", "/opt/airflow/config/ibkr.yaml")
DAG_ID = "ibkr_stream"


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ibkr", "streaming", "quotes"],
)
def ibkr_stream():
    @task
    def stream_quotes_and_features() -> dict:
        from pipeline.config import load_config
        from pipeline.main import build_runner

        config = load_config(CONFIG_PATH)
        runner = build_runner(config)
        summary = runner.run_stream_pipeline()
        logger.info(
            "stream run complete: %s ticks, %s live feature rows",
            summary["ticks"],
            summary["live_feature_rows"],
        )
        return summary

    stream_quotes_and_features()


ibkr_stream()
