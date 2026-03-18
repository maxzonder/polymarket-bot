"""
Telegram alert helper — issue #21.

Single public function: send_message(text).

Configuration via env vars:
  TG_CHAT_ID  — target chat (default: 39371757)

If TG_CHAT_ID is empty, alerts are silently skipped and the bot continues normally.
"""

from __future__ import annotations

import os

import requests

from utils.logger import setup_logger

logger = setup_logger("telegram")

_TOKEN   = "8660557840:AAEOHHZJpF6B5ttQrNIvh-AxKu9LsFuOj4k"
_CHAT_ID = os.getenv("TG_CHAT_ID", "39371757")


def send_message(text: str) -> bool:
    """
    Send a Telegram message to the configured chat.
    Returns True on success, False on failure (never raises).
    """
    if not _CHAT_ID:
        logger.debug("TG_CHAT_ID not set — Telegram alert skipped")
        return False

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{_TOKEN}/sendMessage",
            json={"chat_id": _CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.warning(f"Telegram send_message failed: {e}")
        return False
