import io
import json
import logging

import pytest

from signalkit_stream.logging_utils import JsonLogFormatter, configure_logging


def test_json_formatter_emits_machine_readable_extras() -> None:
    record = logging.LogRecord(
        name="signalkit.runtime",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="source completed",
        args=(),
        exc_info=None,
    )
    record.event = "source.completed"
    record.source_key = "github:issues"
    record.counts = {"events": 3}

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["level"] == "info"
    assert payload["logger"] == "signalkit.runtime"
    assert payload["message"] == "source completed"
    assert payload["event"] == "source.completed"
    assert payload["source_key"] == "github:issues"
    assert payload["counts"] == {"events": 3}
    assert payload["timestamp"].endswith("+00:00")


def test_configure_logging_json_output() -> None:
    stream = io.StringIO()
    configure_logging(output_format="json", stream=stream)
    logger = logging.getLogger("signalkit.test")

    logger.warning("delivery delayed", extra={"event": "delivery.retry", "attempt": 2})
    payload = json.loads(stream.getvalue())

    assert payload["level"] == "warning"
    assert payload["event"] == "delivery.retry"
    assert payload["attempt"] == 2


def test_configure_logging_rejects_unknown_format() -> None:
    with pytest.raises(ValueError, match="output_format"):
        configure_logging(output_format="xml")  # type: ignore[arg-type]
