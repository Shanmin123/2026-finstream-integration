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


def test_reconfiguration_closes_the_replaced_file_handler(tmp_path):
    import logging

    from pipeline.logging_utils import configure_logging

    configure_logging(tmp_path / "logs")
    logger = logging.getLogger("ibkr_pipeline")
    old_file_handler = next(
        h for h in logger.handlers if isinstance(h, logging.FileHandler)
    )
    configure_logging(tmp_path / "logs")
    try:
        assert old_file_handler.stream is None or old_file_handler.stream.closed
        assert len(logger.handlers) == 2
    finally:
        # Undo the global logger state so later tests see a clean slate.
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        logger.propagate = True
