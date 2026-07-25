"""
DAG: IBKR streaming quotes + live features
==========================================
Runs the continuous streaming pipeline as a bounded Airflow task: one long-lived
process per run that subscribes to IBKR quote ticks and, on every flush interval,
appends the ticks to the quotes dataset and recomputes the provisional intraday
indicator/alpha rows for the symbols that moved.

Data flow:
  IB Gateway (reqMktData tick stream) -> quotes parquet     -> quote_ticks
                                      -> live features parquet
                                         -> technical_indicators / alpha_factors
                                            (is_provisional = TRUE)

Latency: the free paper tier streams delayed data (15-20 minutes behind). With
market-data subscriptions the same task streams real time by setting
`stream.market_data_type: 1` in the config — no code change.

The task is bounded by `stream.duration_seconds` so Airflow keeps a normal
task lifecycle; schedule it back-to-back across the session for continuous
coverage (e.g. "*/30 13-20 * * 1-5" with duration_seconds 1800).

Ticks land in the platform's quote_ticks table (immutable observations, so
conflicts are ignored) and the provisional intraday values in
technical_indicators / alpha_factors with is_provisional = TRUE; the end-of-day
run in `ibkr_prices_dag` overwrites them in place because those tables upsert
the newer computation.

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
        if not config.stream.duration_seconds:
            # An Airflow task has to end: with duration 0 the pipeline never
            # returns, so the database-loading steps below would never run and
            # max_active_runs=1 would block every later run.
            raise ValueError(
                "stream.duration_seconds must be positive for the Airflow task; "
                "0 (run until stopped) is for the CLI only."
            )
        runner = build_runner(config)
        summary = runner.run_stream_pipeline()
        from pipeline import db_sink

        quotes = runner.storage.read_latest_quote_rows()
        summary["quote_ticks_loaded"] = db_sink.write_quotes(
            quotes, market_data_type=config.stream.market_data_type
        )
        # Only the session just streamed: pushing every accumulated partition
        # would re-send the whole history of provisional rows on every run.
        def _current_session(dataset):
            frame = runner.storage.read_final_dataset(dataset)
            if frame.empty:
                return frame
            return frame[frame["event_date"] == frame["event_date"].max()]

        summary["indicators_loaded"] = db_sink.write_indicators(
            _current_session("indicators_live")
        )
        summary["alphas_loaded"] = db_sink.write_alphas(
            _current_session("alphas_live")
        )
        logger.info(
            "stream run complete: %s ticks, %s live feature rows",
            summary["ticks"],
            summary["live_feature_rows"],
        )
        return summary

    stream_quotes_and_features()


ibkr_stream()
