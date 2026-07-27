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


def test_zero_stream_duration_is_the_documented_unbounded_mode(tmp_path):
    # README and config.example advertise duration_seconds: 0 as "run until
    # stopped"; a positive-only validator rejected exactly that.
    import yaml

    from pipeline.config import load_config

    base = yaml.safe_load(open("config.example.yaml", encoding="utf-8"))
    base["stream"]["duration_seconds"] = 0
    base["prices"]["symbols"] = ["AAPL"]
    base["prices"].pop("symbols_file", None)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(base), encoding="utf-8")
    assert load_config(str(path)).stream.duration_seconds == 0


def test_negative_stream_duration_is_rejected(tmp_path):
    import pytest as _pytest
    import yaml

    from pipeline.config import ConfigError, load_config

    base = yaml.safe_load(open("config.example.yaml", encoding="utf-8"))
    base["stream"]["duration_seconds"] = -5
    base["prices"]["symbols"] = ["AAPL"]
    base["prices"].pop("symbols_file", None)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(base), encoding="utf-8")
    with _pytest.raises(ConfigError):
        load_config(str(path))


def test_fractional_and_negative_fractional_durations_are_rejected(tmp_path):
    # int() truncation turned -0.5 into 0, silently promoting a rejected value
    # into the unbounded mode.
    import pytest as _pytest
    import yaml

    from pipeline.config import ConfigError, load_config

    for bad in (-0.5, 0.5, 1.9):
        base = yaml.safe_load(open("config.example.yaml", encoding="utf-8"))
        base["stream"]["duration_seconds"] = bad
        base["prices"]["symbols"] = ["AAPL"]
        base["prices"].pop("symbols_file", None)
        path = tmp_path / ("c%s.yaml" % str(bad).replace(".", "_").replace("-", "m"))
        path.write_text(yaml.safe_dump(base), encoding="utf-8")
        with _pytest.raises(ConfigError):
            load_config(str(path))


def _write(tmp_path, mutate, name="c.yaml"):
    import yaml

    base = yaml.safe_load(open("config.example.yaml", encoding="utf-8"))
    base["prices"]["symbols"] = ["AAPL"]
    base["prices"].pop("symbols_file", None)
    mutate(base)
    path = tmp_path / name
    path.write_text(yaml.safe_dump(base), encoding="utf-8")
    return str(path)


def test_share_class_symbols_share_one_canonical_form(tmp_path):
    # BRK.B and BRK-B are one instrument; storing both would split its
    # partitions and break the live-feature join.
    from pipeline.config import canonical_symbol, load_config

    assert canonical_symbol("BRK.B") == canonical_symbol("BRK-B") == "BRK-B"
    path = _write(tmp_path, lambda b: b["stream"].update(symbols=[" brk.b ", "BRK-B", "  "]))
    assert load_config(path).stream.symbols == ("BRK-B",)   # deduped, blanks dropped


def test_non_finite_flush_interval_is_rejected_at_load(tmp_path):
    import pytest as _pytest

    from pipeline.config import ConfigError, load_config

    path = _write(tmp_path, lambda b: b["stream"].update(flush_interval_seconds=float("nan")))
    with _pytest.raises(ConfigError):
        load_config(path)


def test_resolve_universe_distinguishes_unreachable_from_empty():
    """None means the platform database was unreachable -> configured fallback.
    An empty list is a real answer (every company deactivated) and must NOT
    fall back to the snapshot universe."""
    from pipeline.config import resolve_universe

    configured = ("AAPL", "MSFT")
    assert resolve_universe(None, configured) == ("AAPL", "MSFT")
    assert resolve_universe([], configured) == ()
    assert resolve_universe(["brk.b", " ", "aapl"], configured) == ("BRK-B", "AAPL")
    # Aliases of one instrument collapse to a single request.
    assert resolve_universe(["BRK.B", "BRK-B"], configured) == ("BRK-B",)


def test_scalar_where_a_list_belongs_is_rejected_not_iterated(tmp_path):
    """YAML `symbols: AAPL` used to become ('A', 'P', 'L') and
    `use_rth: "false"` used to become True."""
    import pytest as _pytest

    from pipeline.config import ConfigError, load_config

    base = """
ib: {host: "127.0.0.1", port: 4002, client_id: 1}
paths:
  raw_data_dir: raw
  intermediate_data_dir: processed
  output_dir: output
  checkpoint_file: checkpoint.json
  log_dir: logs
prices: {symbols: [AAPL], duration: "1 Y", use_rth: true}
news: {symbols: [AAPL], providers: [BRFG], lookback_days: 1, overlap_minutes: 5, limit: 10}
run: {max_retries: 1, retry_delay_seconds: 0, continue_on_error: true}
""".strip()

    def write(mutation):
        path = tmp_path / "config.yaml"
        path.write_text(base.replace(*mutation), encoding="utf-8")
        return path

    with _pytest.raises(ConfigError, match="symbols must be a list"):
        load_config(write(("symbols: [AAPL], duration", "symbols: AAPL, duration")))
    with _pytest.raises(ConfigError, match="news.providers must be a list"):
        load_config(write(("providers: [BRFG]", "providers: BRFG")))
    # Quoted "false" used to pass through bool() as True; it now parses.
    config = load_config(write(("use_rth: true", 'use_rth: "false"')))
    assert config.prices.use_rth is False
    with _pytest.raises(ConfigError, match="prices.use_rth"):
        load_config(write(("use_rth: true", "use_rth: maybe")))
    with _pytest.raises(ConfigError, match="ib.request_timeout_seconds"):
        load_config(write(("client_id: 1}", "client_id: 1, request_timeout_seconds: -1}")))
    with _pytest.raises(ConfigError, match="ib.connect_timeout_seconds"):
        load_config(write(("client_id: 1}", "client_id: 1, connect_timeout_seconds: 0}")))
    # Sane string booleans still parse rather than forcing YAML-native only.
    config = load_config(write(("continue_on_error: true", 'continue_on_error: "false"')))
    assert config.run.continue_on_error is False


def test_news_max_pages_is_configurable_and_validated(tmp_path):
    from pipeline.config import ConfigError, load_config

    path = _write(tmp_path, lambda b: b["news"].update(max_pages=9))
    assert load_config(path).news.max_pages == 9
    import pytest as _pytest

    path = _write(tmp_path, lambda b: b["news"].update(max_pages=0), name="c0.yaml")
    with _pytest.raises(ConfigError, match="news.max_pages"):
        load_config(path)


def test_out_of_range_and_non_integral_values_are_rejected(tmp_path):
    import math

    import pytest as _pytest

    from pipeline.config import ConfigError, load_config

    cases = [
        (lambda b: b["ib"].update(port=70000), "ib.port"),
        (lambda b: b["ib"].update(client_id=-1), "ib.client_id"),
        (lambda b: b["prices"].update(duration="5Q"), "prices.duration"),
        (lambda b: b["stream"].update(market_data_type=99), "stream.market_data_type"),
        (lambda b: b["run"].update(retry_delay_seconds=math.nan), "run.retry_delay_seconds"),
        (lambda b: b["run"].update(max_retries=1.9), "max_retries"),
    ]
    for index, (mutate, field) in enumerate(cases):
        path = _write(tmp_path, mutate, name="bad%d.yaml" % index)
        with _pytest.raises(ConfigError, match=field.replace(".", r"\.")):
            load_config(path)
    # A null YAML symbol entry is dropped, never the ticker "NONE".
    path = _write(tmp_path, lambda b: b["stream"].update(symbols=[None, "AAPL"]),
                  name="nullsym.yaml")
    assert load_config(path).stream.symbols == ("AAPL",)
