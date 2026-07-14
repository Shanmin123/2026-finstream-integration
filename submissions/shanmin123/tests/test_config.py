from pathlib import Path

import pytest

from pipeline.config import ConfigError, load_config


def test_load_config_resolves_paths_and_deduplicates_symbols(config_file):
    config = load_config(config_file)

    assert config.ib.host == "127.0.0.1"
    assert config.ib.port == 4002
    assert config.prices.symbols == ("AAPL", "MSFT")
    assert config.news.symbols == ("AAPL",)
    assert config.paths.raw_data_dir == config_file.parent / "data" / "raw"
    assert (
        config.paths.checkpoint_file == config_file.parent / "data" / ".checkpoint.json"
    )


def test_load_config_allows_environment_connection_overrides(config_file, monkeypatch):
    monkeypatch.setenv("IB_HOST", "gateway.internal")
    monkeypatch.setenv("IB_PORT", "7497")
    monkeypatch.setenv("IB_CLIENT_ID", "87")

    config = load_config(config_file)

    assert config.ib.host == "gateway.internal"
    assert config.ib.port == 7497
    assert config.ib.client_id == 87


def test_load_config_rejects_missing_news_providers(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
ib: {host: "127.0.0.1", port: 4002, client_id: 1}
paths:
  raw_data_dir: raw
  intermediate_data_dir: processed
  output_dir: output
  checkpoint_file: checkpoint.json
  log_dir: logs
prices: {symbols: [AAPL], duration: "1 Y", use_rth: true}
news: {symbols: [AAPL], providers: [], lookback_days: 1, overlap_minutes: 5, limit: 10}
run: {max_retries: 1, retry_delay_seconds: 0, continue_on_error: true}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="news.providers"):
        load_config(config_path)


def test_config_module_uses_python38_compatible_annotations():
    source = Path(__file__).resolve().parents[1] / "pipeline" / "config.py"
    text = source.read_text(encoding="utf-8")

    assert " | None" not in text
    assert "list[" not in text
