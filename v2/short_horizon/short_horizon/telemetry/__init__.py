"""Telemetry boundary for structured logging and run-scoped diagnostics."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, TextIO

MANDATORY_FIELDS = ("run_id", "market_id", "order_id", "event_time", "ingest_time")
_HANDLER_MARKER = "_short_horizon_structured"


class StructuredJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        structured = dict(getattr(record, "_structured", {}))
        payload: dict[str, Any] = {
            "logger": record.name,
            "level": record.levelname.lower(),
            "message": record.getMessage(),
            "event": structured.pop("event", record.getMessage()),
        }
        for field in MANDATORY_FIELDS:
            payload[field] = structured.pop(field, None)
        payload.update(structured)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True, default=_json_default)


class StructuredLogger:
    def __init__(self, logger: logging.Logger, *, context: dict[str, Any] | None = None):
        self._logger = logger
        self._context = dict(context or {})

    def bind(self, **context: Any) -> "StructuredLogger":
        merged = dict(self._context)
        for key, value in context.items():
            if value is not None:
                merged[key] = value
        return StructuredLogger(self._logger, context=merged)

    def debug(self, event: str, **fields: Any) -> None:
        self._log(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._log(logging.ERROR, event, **fields)

    def exception(self, event: str, **fields: Any) -> None:
        self._log(logging.ERROR, event, exc_info=True, **fields)

    def _log(self, level: int, event: str, *, exc_info: bool = False, **fields: Any) -> None:
        payload = {field: None for field in MANDATORY_FIELDS}
        payload.update(self._context)
        payload.update({key: value for key, value in fields.items() if value is not None})
        payload["event"] = event
        self._logger.log(level, event, extra={"_structured": payload}, exc_info=exc_info)


def configure_logging(
    name: str = "short_horizon",
    *,
    level: int | str = logging.INFO,
    stream: TextIO | None = None,
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
    handler.setFormatter(StructuredJsonFormatter())
    logger.addHandler(handler)
    return logger


def get_logger(name: str, **context: Any) -> StructuredLogger:
    return StructuredLogger(logging.getLogger(name), context=context)


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


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return str(value)


__all__ = [
    "MANDATORY_FIELDS",
    "StructuredJsonFormatter",
    "StructuredLogger",
    "configure_logging",
    "event_log_fields",
    "get_logger",
    "iso_from_ms",
]
