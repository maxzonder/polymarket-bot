"""Telemetry boundary for structured logging and run-scoped diagnostics."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, TextIO

import structlog

MANDATORY_FIELDS = ("run_id", "market_id", "order_id", "event_time", "ingest_time")
_HANDLER_MARKER = "_short_horizon_structured"


def configure_logging(
    name: str = "short_horizon",
    *,
    level: int | str = logging.INFO,
    stream: TextIO | None = None,
    alert_handler: logging.Handler | None = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    for handler in list(logger.handlers):
        if getattr(handler, _HANDLER_MARKER, False):
            logger.removeHandler(handler)
            handler.close()
    handler = logging.StreamHandler(stream)
    setattr(handler, _HANDLER_MARKER, True)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    if alert_handler is not None:
        setattr(alert_handler, _HANDLER_MARKER, True)
        logger.addHandler(alert_handler)

    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            _inject_mandatory_fields,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=_json_dumps),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return logger


def get_logger(name: str, **context: Any) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name).bind(**_drop_none(context))


def event_log_fields(event: Any, *, order_id: str | None = None) -> dict[str, Any]:
    return {
        "market_id": getattr(event, "market_id", None),
        "order_id": order_id if order_id is not None else getattr(event, "order_id", None),
        "event_time": iso_from_ms(getattr(event, "event_time_ms", None)),
        "ingest_time": iso_from_ms(getattr(event, "ingest_time_ms", None)),
        "token_id": getattr(event, "token_id", None),
        "event_type": getattr(getattr(event, "event_type", None), "value", getattr(event, "event_type", None)),
        "source": getattr(event, "source", None),
    }


def iso_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _inject_mandatory_fields(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    for field in MANDATORY_FIELDS:
        event_dict.setdefault(field, None)
    return _drop_none(event_dict)


def _drop_none(values: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None}


def _json_dumps(payload: Any, **_: Any) -> str:
    return json.dumps(payload, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return str(value)


__all__ = [
    "MANDATORY_FIELDS",
    "configure_logging",
    "event_log_fields",
    "get_logger",
    "iso_from_ms",
]
