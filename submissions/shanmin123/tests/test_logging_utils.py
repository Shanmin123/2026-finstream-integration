import json
import logging

from pipeline.logging_utils import JsonFormatter


def test_json_formatter_preserves_structured_pipeline_fields():
    record = logging.LogRecord(
        name="ibkr_pipeline",
        level=logging.WARNING,
        pathname=__file__,
        lineno=10,
        msg="ib_request_retry",
        args=(),
        exc_info=None,
    )
    record.dataset = "news"
    record.symbol = "AAPL"
    record.attempt = 2

    payload = json.loads(JsonFormatter().format(record))

    assert payload["event"] == "ib_request_retry"
    assert payload["level"] == "WARNING"
    assert payload["dataset"] == "news"
    assert payload["symbol"] == "AAPL"
    assert payload["attempt"] == 2
