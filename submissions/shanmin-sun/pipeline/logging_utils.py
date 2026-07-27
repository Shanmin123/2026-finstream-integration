import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path


STRUCTURED_FIELDS = (
    "dataset",
    "symbol",
    "rows",
    "operation",
    "attempt",
    "error",
)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp_utc": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for field in STRUCTURED_FIELDS:
            if hasattr(record, field):
                payload[field] = getattr(record, field)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True, default=str)


def configure_logging(log_dir):
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    log_path = path / "ibkr_pipeline.jsonl"
    formatter = JsonFormatter()

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger = logging.getLogger("ibkr_pipeline")
    # Close what is being replaced: repeat configuration (tests, embedded
    # callers) would otherwise leak an open file handle per call.
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return log_path
