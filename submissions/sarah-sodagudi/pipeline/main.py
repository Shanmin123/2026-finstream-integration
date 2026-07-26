"""
Fundamental data pipeline: EODHD -> MongoDB.

Follows the patterns required by the integration guidelines:
- config loaded from separate files, not hardcoded -- secrets in .env,
  everything else (hosts, ports, paths, retry/batch params) in config.yaml
- logging for diagnosability
- checkpoint-based resume after interruption

Fetches annual + quarterly fundamentals for every active S&P 500
constituent from the EODHD Fundamental Data API and writes every numeric
field reported per fiscal period (no fixed taxonomy) into MongoDB.
"""
import json
import logging
import os
from pathlib import Path

import yaml

from eodhd_client import fetch_fundamentals
from fundamentals_mapper import extract_all_periods
from mongo_writer import push_ticker_periods
from ticker_universe import get_ticker_universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def load_env_file(path: str = ".env") -> None:
    """
    Minimal .env parser (KEY=VALUE per line, '#' comments and blank lines
    skipped) -- deliberately no python-dotenv dependency, this repo's
    pinned-stack policy requires justifying any new library before adding
    it, and a few lines of stdlib parsing covers what's needed here.
    Existing real environment variables always take precedence over the
    file (os.environ.setdefault, not direct assignment) -- e.g. CI or a
    production scheduler injecting EODHD_API_TOKEN directly still wins
    over anything committed to .env.example / left in a local .env.
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

    POSTGRES_HOST (optional env var) overrides config.yaml's postgres.host
    if set -- config.yaml's own value is correct for running `python
    main.py` directly on the host (postgres.host: "localhost" reaches a
    Postgres bound to the host machine), but wrong from inside the
    Airflow container, where "localhost" means the container itself, not
    the host. docker-compose.yml sets POSTGRES_HOST=host.docker.internal
    for the containerised run so the SAME config.yaml works for both
    without editing it back and forth between run modes.
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


def load_checkpoint(checkpoint_file: str) -> dict:
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            logger.info("Resuming from existing checkpoint: %s", checkpoint_file)
            return json.load(f)
    return {"completed_tickers": []}


def save_checkpoint(checkpoint_file: str, state: dict) -> None:
    Path(os.path.dirname(checkpoint_file) or ".").mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, "w") as f:
        json.dump(state, f)


def collect(config: dict, checkpoint: dict):
    """
    Fetches raw EODHD fundamentals JSON for every active ticker not yet
    marked complete in the checkpoint. Checkpoint is saved after EACH
    ticker (not just at the end) so an interruption mid-run only costs
    the one ticker in flight, not the whole batch.
    """
    universe = get_ticker_universe(config)

    ticker_limit = config.get("run", {}).get("ticker_limit", 0)
    if ticker_limit:
        logger.warning("run.ticker_limit=%d set -- processing only the first %d of %d tickers.",
                        ticker_limit, ticker_limit, len(universe))
        universe = universe[:ticker_limit]

    completed = set(checkpoint.get("completed_tickers", []))
    remaining = [(t, e) for t, e in universe if t not in completed]
    logger.info("%d/%d tickers remaining (%d already completed per checkpoint).",
                len(remaining), len(universe), len(completed))

    checkpoint_file = config["paths"]["checkpoint_file"]
    raw_data = []

    for ticker, exchange in remaining:
        raw = fetch_fundamentals(ticker, exchange, config)
        if raw is not None:
            raw_data.append((ticker, exchange, raw))
        else:
            logger.warning("No data for %s.%s -- skipping, not marking complete (will retry next run).", ticker, exchange)
            continue

        checkpoint.setdefault("completed_tickers", []).append(ticker)
        save_checkpoint(checkpoint_file, checkpoint)

    return raw_data


def process(raw_data):
    """Flattens each ticker's raw EODHD document into annual + quarterly
    per-period metrics (see fundamentals_mapper.py)."""
    logger.info("Processing raw data for %d tickers", len(raw_data))
    processed = []
    for ticker, exchange, raw in raw_data:
        periods = extract_all_periods(raw)
        processed.append((ticker, exchange, periods))
    return processed


def save(processed_data, config: dict):
    """Upserts every period into MongoDB (see mongo_writer.py)."""
    logger.info("Saving fundamentals for %d tickers to MongoDB", len(processed_data))
    totals = {"tickers": 0, "annual_pushed": 0, "quarterly_pushed": 0}
    for ticker, exchange, periods in processed_data:
        result = push_ticker_periods(ticker, exchange, periods, config)
        totals["tickers"] += 1
        totals["annual_pushed"] += result["annual_pushed"]
        totals["quarterly_pushed"] += result["quarterly_pushed"]
    logger.info("Save complete: %s", totals)
    return totals


def main():
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
        # Full successful run: reset the checkpoint so the NEXT invocation
        # is a fresh refresh cycle (this pipeline is periodic, not a
        # one-shot backfill -- fundamentals get re-pulled to pick up new
        # filings, not skipped forever once done once).
        save_checkpoint(config["paths"]["checkpoint_file"], {"completed_tickers": []})
        logger.info("Pipeline completed successfully")
        return totals


if __name__ == "__main__":
    main()
