import argparse
import json
import logging
import sys

from pipeline.checkpoint import CheckpointStore
from pipeline.config import load_config
from pipeline.ibapi_client import IBApiClient
from pipeline.logging_utils import configure_logging
from pipeline.runner import PipelineRunner
from pipeline.storage import LayeredStorage


def build_parser():
    parser = argparse.ArgumentParser(
        description="Collect IBKR daily OHLCV and news headlines through IB Gateway."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the YAML pipeline configuration file.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("smoke-test", help="Check Gateway, prices, and news.")
    subparsers.add_parser("collect-prices", help="Collect daily OHLCV.")
    subparsers.add_parser("collect-news", help="Collect news headlines.")
    subparsers.add_parser("run-all", help="Collect prices and news.")
    subparsers.add_parser(
        "compute-features",
        help="Compute technical indicators and the Alpha101 subset from final prices.",
    )
    return parser


def build_runner(config):
    client = IBApiClient(
        config.ib.host,
        config.ib.port,
        config.ib.client_id,
        request_timeout_seconds=config.ib.request_timeout_seconds,
        connect_timeout_seconds=config.ib.connect_timeout_seconds,
    )
    storage = LayeredStorage(
        config.paths.raw_data_dir,
        config.paths.intermediate_data_dir,
        config.paths.output_dir,
    )
    checkpoint = CheckpointStore(config.paths.checkpoint_file)
    return PipelineRunner(config, client, storage, checkpoint)


def main(argv=None):
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    configure_logging(config.paths.log_dir)
    logger = logging.getLogger("ibkr_pipeline")
    runner = build_runner(config)

    commands = {
        "smoke-test": runner.smoke_test,
        "collect-prices": runner.run_prices,
        "collect-news": runner.run_news,
        "run-all": runner.run_all,
        "compute-features": runner.run_features,
    }
    try:
        result = commands[args.command]()
    except Exception as exc:
        logger.exception("pipeline_failed")
        print(
            json.dumps(
                {"status": "failed", "error": str(exc), "type": type(exc).__name__}
            ),
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    if _result_has_errors(result):
        return 1
    return 0


def _result_has_errors(result):
    if isinstance(result, dict) and result.get("errors"):
        return True
    if isinstance(result, dict):
        return any(
            isinstance(value, dict) and value.get("errors") for value in result.values()
        )
    return False


if __name__ == "__main__":
    sys.exit(main())
