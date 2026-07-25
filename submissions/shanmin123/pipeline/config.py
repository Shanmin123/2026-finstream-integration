import csv
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import yaml


class ConfigError(ValueError):
    """Raised when a pipeline configuration is incomplete or invalid."""


@dataclass(frozen=True)
class IBConfig:
    host: str
    port: int
    client_id: int
    connect_timeout_seconds: float
    request_timeout_seconds: float


@dataclass(frozen=True)
class PathsConfig:
    raw_data_dir: Path
    intermediate_data_dir: Path
    output_dir: Path
    checkpoint_file: Path
    log_dir: Path


@dataclass(frozen=True)
class PricesConfig:
    symbols: Tuple[str, ...]
    duration: str
    use_rth: bool


@dataclass(frozen=True)
class NewsConfig:
    symbols: Tuple[str, ...]
    providers: Tuple[str, ...]
    lookback_days: int
    overlap_minutes: int
    limit: int


@dataclass(frozen=True)
class StreamConfig:
    symbols: tuple
    duration_seconds: int
    market_data_type: int
    flush_interval_seconds: float


@dataclass(frozen=True)
class RunConfig:
    max_retries: int
    retry_delay_seconds: float
    continue_on_error: bool
    symbol_delay_seconds: float


@dataclass(frozen=True)
class PipelineConfig:
    ib: IBConfig
    paths: PathsConfig
    prices: PricesConfig
    news: NewsConfig
    run: RunConfig
    stream: StreamConfig
    config_path: Path


def _section(data, name):
    value = data.get(name)
    if not isinstance(value, dict):
        raise ConfigError("Missing or invalid '%s' section" % name)
    return value


def _resolve_path(base_dir, value, field_name):
    if not value:
        raise ConfigError("Missing path: %s" % field_name)
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _canonical_symbols(values):
    seen = set()
    symbols = []
    for value in values or []:
        symbol = str(value).strip().upper()
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return tuple(symbols)


def _symbols_from_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "symbol" not in reader.fieldnames:
            raise ConfigError("symbols_file must contain a 'symbol' column")
        return [row.get("symbol", "") for row in reader]


def _load_symbols(section, base_dir, fallback=None):
    values = list(section.get("symbols") or [])
    symbols_file = section.get("symbols_file")
    if symbols_file:
        path = _resolve_path(base_dir, symbols_file, "symbols_file")
        if not path.exists():
            raise ConfigError("symbols_file does not exist: %s" % path)
        values.extend(_symbols_from_csv(path))
    symbols = _canonical_symbols(values)
    if not symbols and fallback:
        symbols = tuple(fallback)
    return symbols


def _positive_int(value, field_name):
    number = int(value)
    if number <= 0:
        raise ConfigError("%s must be greater than zero" % field_name)
    return number


def load_config(path):
    config_path = Path(path).expanduser().resolve()
    if not config_path.exists():
        raise ConfigError("Configuration file does not exist: %s" % config_path)
    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigError("Configuration root must be a mapping")

    base_dir = config_path.parent
    ib_data = _section(data, "ib")
    paths_data = _section(data, "paths")
    prices_data = _section(data, "prices")
    news_data = _section(data, "news")
    run_data = _section(data, "run")

    price_symbols = _load_symbols(prices_data, base_dir)
    if not price_symbols:
        raise ConfigError("prices must define symbols or symbols_file")
    news_symbols = _load_symbols(news_data, base_dir, fallback=price_symbols)
    providers = _canonical_symbols(news_data.get("providers"))
    if not providers:
        raise ConfigError("news.providers must contain at least one provider code")

    ib = IBConfig(
        host=os.getenv("IB_HOST", str(ib_data.get("host", "127.0.0.1"))),
        port=int(os.getenv("IB_PORT", str(ib_data.get("port", 4002)))),
        client_id=int(os.getenv("IB_CLIENT_ID", str(ib_data.get("client_id", 31)))),
        connect_timeout_seconds=float(ib_data.get("connect_timeout_seconds", 10)),
        request_timeout_seconds=float(ib_data.get("request_timeout_seconds", 30)),
    )
    paths = PathsConfig(
        raw_data_dir=_resolve_path(
            base_dir, paths_data.get("raw_data_dir"), "paths.raw_data_dir"
        ),
        intermediate_data_dir=_resolve_path(
            base_dir,
            paths_data.get("intermediate_data_dir"),
            "paths.intermediate_data_dir",
        ),
        output_dir=_resolve_path(
            base_dir, paths_data.get("output_dir"), "paths.output_dir"
        ),
        checkpoint_file=_resolve_path(
            base_dir, paths_data.get("checkpoint_file"), "paths.checkpoint_file"
        ),
        log_dir=_resolve_path(base_dir, paths_data.get("log_dir"), "paths.log_dir"),
    )
    prices = PricesConfig(
        symbols=price_symbols,
        duration=str(prices_data.get("duration", "5 Y")),
        use_rth=bool(prices_data.get("use_rth", True)),
    )
    news = NewsConfig(
        symbols=news_symbols,
        providers=providers,
        lookback_days=_positive_int(news_data.get("lookback_days", 1), "lookback_days"),
        overlap_minutes=max(int(news_data.get("overlap_minutes", 5)), 0),
        limit=_positive_int(news_data.get("limit", 100), "news.limit"),
    )
    run = RunConfig(
        max_retries=_positive_int(run_data.get("max_retries", 3), "max_retries"),
        retry_delay_seconds=max(float(run_data.get("retry_delay_seconds", 2)), 0.0),
        continue_on_error=bool(run_data.get("continue_on_error", True)),
        symbol_delay_seconds=max(
            float(run_data.get("symbol_delay_seconds", 1.0)), 0.0
        ),
    )
    stream_data = data.get("stream") or {}
    # An absent or empty stream section is fine for the batch commands; the
    # streaming commands check for symbols when they actually run, so a config
    # that never streams does not have to carry a stream block.
    # Upper-cased like the price and news universes: the live-feature join
    # against the price history is case-sensitive.
    stream_symbols = tuple(
        str(symbol).strip().upper() for symbol in (stream_data.get("symbols") or ())
    )
    # 0 is the documented "run until stopped" mode, so this is not _positive_int.
    # Validate before int(), or -0.5 truncates to 0 and silently becomes unbounded.
    raw_duration = stream_data.get("duration_seconds", 60)
    try:
        duration_value = float(raw_duration)
    except (TypeError, ValueError):
        raise ConfigError("stream.duration_seconds must be a number")
    if not math.isfinite(duration_value):
        raise ConfigError("stream.duration_seconds must be a finite number")
    if duration_value < 0:
        raise ConfigError("stream.duration_seconds must be 0 (unbounded) or positive")
    if duration_value != int(duration_value):
        raise ConfigError("stream.duration_seconds must be a whole number of seconds")
    stream_duration = int(duration_value)
    stream = StreamConfig(
        symbols=stream_symbols,
        duration_seconds=stream_duration,
        market_data_type=int(stream_data.get("market_data_type", 3)),
        flush_interval_seconds=max(
            float(stream_data.get("flush_interval_seconds", 5.0)), 0.1
        ),
    )
    return PipelineConfig(
        ib=ib,
        stream=stream,
        paths=paths,
        prices=prices,
        news=news,
        run=run,
        config_path=config_path,
    )
