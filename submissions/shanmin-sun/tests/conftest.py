import sys
from pathlib import Path

import pytest


SUBMISSION_ROOT = Path(__file__).resolve().parents[1]
if str(SUBMISSION_ROOT) not in sys.path:
    sys.path.insert(0, str(SUBMISSION_ROOT))


@pytest.fixture
def config_file(tmp_path):
    symbols_file = tmp_path / "symbols.csv"
    symbols_file.write_text("symbol\nAAPL\nMSFT\nAAPL\n", encoding="utf-8")

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
ib:
  host: "127.0.0.1"
  port: 4002
  client_id: 31
  connect_timeout_seconds: 5
  request_timeout_seconds: 10
paths:
  raw_data_dir: "./data/raw"
  intermediate_data_dir: "./data/processed"
  output_dir: "./data/output"
  checkpoint_file: "./data/.checkpoint.json"
  log_dir: "./logs"
prices:
  symbols_file: "./symbols.csv"
  duration: "3 Y"
  use_rth: true
news:
  symbols: ["AAPL"]
  providers: ["BRFG", "BRFUPDN", "DJNL"]
  lookback_days: 2
  overlap_minutes: 5
  limit: 100
run:
  max_retries: 2
  retry_delay_seconds: 0
  continue_on_error: true
""".strip(),
        encoding="utf-8",
    )
    return config_path


@pytest.fixture
def pipeline_config(config_file):
    from pipeline.config import load_config

    return load_config(config_file)
