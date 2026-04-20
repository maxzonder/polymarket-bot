"""Telemetry boundary for structured logging and run-scoped diagnostics."""

import logging


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


__all__ = ["get_logger"]
