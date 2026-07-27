"""
DAG: IBKR daily price + feature ingestion
=========================================
Collects daily OHLCV for the active universe through IB Gateway (TWS API),
derives the technical-indicator and Alpha101-subset datasets, and upserts the
bars into the platform's PostgreSQL `price_data` table with source='ibkr'.

Data flow:
  IB Gateway (TWS API socket) -> collect-prices    -> parquet layers
                              -> compute-features  -> indicator/alpha parquet
                              -> price_data, technical_indicators, alpha_factors
                                 (PostgreSQL, source='ibkr')

Universe: pulled from the platform (`companies.is_active`) so the DAG does not
hardcode a list; falls back to the configured symbols file when the database is
unreachable. This is the CURRENT membership, which is what the daily
incremental run needs.

Known limitation, not solved here: `alpha_20` ranks cross-sectionally over
whichever symbols are in the panel. The symbols file is a snapshot of names
trading at a single date, not dated membership, so a multi-year backfill ranks
past dates against a later constituent set. Removing that bias needs a
membership table with effective dates (the platform's `companies` table would
be the natural home); until then, treat historical `alpha_20` as
survivorship-affected and prefer the daily forward-computed values.

Requires an authenticated IB Gateway paper session reachable from the worker at
IB_HOST/IB_PORT (the login itself is interactive and is not automated here).
Pacing: the collector sleeps `run.symbol_delay_seconds` between symbols, so a
full S&P-500 sweep stays under IBKR historical-data pacing limits.

Cadence and source precedence: `price_data`'s unique key excludes `source` and
both feeds insert with DO NOTHING, so whichever writes a ticker/day first wins.
This feed is documented as a GAP FILLER, which is only true if it runs AFTER the
primary EODHD load (that DAG runs 04:00 Tue-Sat). Scheduling this at 23:00 the
previous evening would invert that and make IBKR preempt EODHD. When enabled,
set "0 6 * * 2-6" (after EODHD completes) rather than an evening slot, or agree
an explicit precedence with the integration team.

`schedule=None` during validation.
"""

from __future__ import annotations

import logging
import os
import time

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
    # The parquet layer is single-writer per dataset (see storage.py); a
    # second concurrent run could snapshot-merge away the first one's rows.
    max_active_runs=1,
    tags=["ibkr", "prices", "alpha"],
)
def ibkr_prices():
    @task
    def collect_prices() -> dict:
        from pipeline import db_sink

        started = time.time()
        config, runner = _build_runner()
        from pipeline.config import resolve_universe

        active = db_sink.get_active_tickers()
        if active is not None:
            # The platform answered: its membership is the truth, including an
            # empty list (every company deactivated -> collect nothing). Only
            # an unreachable database falls back to the configured file.
            symbols = resolve_universe(active, config.prices.symbols)
            from dataclasses import replace

            runner.config = replace(
                config, prices=replace(config.prices, symbols=symbols)
            )
            logger.info("universe from platform: %d tickers", len(symbols))
        summary = runner.run_prices()
        summary["started_epoch"] = started
        return summary

    @task
    def compute_features(_collected: dict) -> dict:
        _config, runner = _build_runner()
        return runner.run_features()

    @task
    def load_features_to_postgres(_loaded: int) -> dict:
        """Upsert the newest session's derived rows into the platform tables.

        Reads what compute_features already wrote rather than recomputing it,
        and pushes only the latest session: sending the whole panel would be
        ~17 million long-format rows per daily run, nearly all of them
        unchanged. The read is scoped to the newest year partition so the
        task's parquet I/O does not grow with accumulated history either.
        """
        from pipeline import db_sink

        _config, runner = _build_runner()

        def _latest(dataset):
            frame = runner.storage.read_final_dataset(dataset, latest_year_only=True)
            if frame.empty:
                return frame
            return frame[frame["event_date"] == frame["event_date"].max()]

        return {
            "indicators": db_sink.write_indicators(_latest("indicators")),
            "alphas": db_sink.write_alphas(_latest("alphas")),
        }

    @task
    def load_price_data_to_postgres(collected: dict, _features: dict) -> int:
        """Upsert the bars this run actually collected into price_data.

        The watermark is the PANEL-WIDE earliest event_date this run
        fetched: a daily increment pushes only the few overlap days, while a
        single symbol's deep backfill re-offers the whole panel from that
        date (DO NOTHING absorbs the overlap; the cost is transport, not
        correctness). Pushing one panel-wide max date instead would load a
        single session on a backfill and would skip a suspended ticker whose
        newest bar predates the panel maximum.
        """
        from pipeline import db_sink

        _config, runner = _build_runner()
        if os.environ.get("IBKR_PRICE_LOAD_FULL", "").lower() == "true":
            # Recovery lever: if a worker died between collecting and loading
            # (parquet advanced, price_data did not), the watermark of the
            # NEXT run no longer covers the stranded bars. Pushing the whole
            # panel once is safe -- DO NOTHING absorbs every existing row.
            prices = runner.storage.read_final_prices()
            if prices.empty:
                return 0
            return db_sink.write_prices(prices.to_dict("records"))
        watermark = collected.get("earliest_event_date")
        if not watermark:
            return 0
        prices = runner.storage.read_final_prices()
        if prices.empty:
            return 0
        rows = prices[prices["event_date"] >= watermark].to_dict("records")
        return db_sink.write_prices(rows)

    @task
    def record_telemetry(collected: dict, _features_loaded: dict) -> None:
        """One pipeline_runs row per fully successful chain.

        Recorded LAST so a load failure cannot leave a success row behind;
        a failed run leaves no row, matching the collection-failure case.
        """
        from pipeline import db_sink

        try:
            db_sink.record_run(
                DAG_ID,
                collected["written"],
                time.time() - collected["started_epoch"],
                "success" if not collected["errors"] else "partial",
                None if not collected["errors"] else str(collected["errors"][:3]),
            )
        except Exception:
            # The database is optional: telemetry must not fail a run whose
            # datasets already landed.
            logger.warning("pipeline_runs telemetry unavailable", exc_info=True)

    collected = collect_prices()
    features = compute_features(collected)
    record_telemetry(
        collected, load_features_to_postgres(load_price_data_to_postgres(collected, features))
    )


ibkr_prices()
