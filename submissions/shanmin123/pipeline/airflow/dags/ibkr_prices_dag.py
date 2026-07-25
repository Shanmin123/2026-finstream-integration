"""
DAG: IBKR daily price + feature ingestion
=========================================
Collects daily OHLCV for the active universe through IB Gateway (TWS API),
derives the technical-indicator and Alpha101-subset datasets, and upserts the
bars into the platform's PostgreSQL `price_data` table with source='ibkr'.

Data flow:
  IB Gateway (TWS API socket) -> collect-prices -> parquet layers
                              -> compute-features -> indicator/alpha parquet
                              -> price_data (PostgreSQL, source='ibkr')

Universe: pulled from the platform (`companies.is_active`) so the DAG does not
hardcode a list; falls back to the configured symbols file when the database is
unreachable.

Requires an authenticated IB Gateway paper session reachable from the worker at
IB_HOST/IB_PORT (the login itself is interactive and is not automated here).
Pacing: the collector sleeps `run.symbol_delay_seconds` between symbols, so a
full S&P-500 sweep stays under IBKR historical-data pacing limits.

Cadence: daily bars settle at the US close, so this runs after it.
`schedule=None` during validation; set to "0 23 * * 1-5" when enabled.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("IBKR_PIPELINE_CONFIG", "/opt/airflow/config/ibkr.yaml")
DAG_ID = "ibkr_prices"


def _build_runner():
    from pipeline.config import load_config
    from pipeline.main import build_runner

    config = load_config(CONFIG_PATH)
    return config, build_runner(config)


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ibkr", "prices", "alpha"],
)
def ibkr_prices():
    @task
    def collect_prices() -> int:
        from pipeline import db_sink

        started = time.time()
        config, runner = _build_runner()
        active = db_sink.get_active_tickers()
        if active:
            from dataclasses import replace

            runner.config = replace(
                config, prices=replace(config.prices, symbols=tuple(active))
            )
            logger.info("universe from platform: %d tickers", len(active))
        summary = runner.run_prices()
        db_sink.record_run(
            DAG_ID,
            summary["written"],
            time.time() - started,
            "success" if not summary["errors"] else "partial",
            None if not summary["errors"] else str(summary["errors"][:3]),
        )
        return summary["written"]

    @task
    def compute_features(_written: int) -> dict:
        _config, runner = _build_runner()
        return runner.run_features()

    @task
    def load_price_data_to_postgres(_features: dict) -> int:
        """Upsert the freshly collected bars into the platform price_data table."""
        from pipeline import db_sink

        _config, runner = _build_runner()
        prices = runner.storage.read_final_prices()
        if prices.empty:
            return 0
        today = datetime.utcnow().strftime("%Y-%m-%d")
        recent = prices[prices["event_date"] >= today]
        rows = (recent if not recent.empty else prices.tail(1000)).to_dict("records")
        return db_sink.write_prices(rows)

    load_price_data_to_postgres(compute_features(collect_prices()))


ibkr_prices()
