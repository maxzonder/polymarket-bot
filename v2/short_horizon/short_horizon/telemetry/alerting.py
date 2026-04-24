import json
import logging
import urllib.request
import urllib.error
from typing import Any

from . import get_logger

logger = get_logger("short_horizon.telemetry.alerting")

class TelegramWebhookHandler(logging.Handler):
    """
    Sends designated log records to a Telegram chat via standard bot API webhook.
    Requires bot_token and chat_id to be configured.
    """
    def __init__(self, bot_token: str, chat_id: str, level: int = logging.INFO):
        super().__init__(level)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            if not msg:
                return
            payload = {
                "chat_id": self.chat_id,
                "text": msg,
                "parse_mode": "HTML"
            }
            req = urllib.request.Request(
                self.url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                pass
        except urllib.error.HTTPError as e:
            if e.code == 404 or e.code == 401:
                pass # Fake/invalid tokens, ignore
            else:
                self.handleError(record)
        except Exception as e:
            self.handleError(record)

def create_telegram_alert_handler(bot_token: str | None, chat_id: str | None) -> logging.Handler | None:
    if not bot_token or not chat_id:
        return None
    
    # Custom formatter to extract structlog kwargs if present and format a readable alert
    class TelegramAlertFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            # We assume structlog passes a JSON string or dict as the message, but
            # since structlog's JSONRenderer is at the end of the processor chain,
            # record.msg might be the JSON string.
            try:
                if isinstance(record.msg, str):
                    data = json.loads(record.msg)
                elif isinstance(record.msg, dict):
                    data = record.msg
                else:
                    data = {"event": str(record.msg)}
            except Exception:
                data = {"event": str(record.msg)}

            event_name = data.get("event", "alert")
            level = record.levelname
            
            # Simple bold header
            lines = [f"<b>[{level}] {event_name}</b>"]
            
            # Determine if this event is interesting enough for an alert
            # We alert on ERROR/CRITICAL, or specific lifecycle events
            interesting_events = {
                "live_order_accepted",
                "live_order_filled",
                "live_order_rejected",
                "live_cancel_submit_failed",
                "risk_limit_breached",
                "live_stub_run_completed",
                "kill_switch_executed",
                "live_order_canceled"
            }
            if level not in ("ERROR", "CRITICAL") and event_name not in interesting_events:
                # By returning an empty string, the Handler won't send it (we can add a check)
                return ""

            
            # Skip noise fields, include important ones
            skip_fields = {"event", "logger", "level", "event_time", "ingest_time", "source"}
            for k, v in data.items():
                if k not in skip_fields and v is not None:
                    lines.append(f"• <b>{k}</b>: {v}")
            
            return "\n".join(lines)

    handler = TelegramWebhookHandler(bot_token, chat_id)
    handler.setFormatter(TelegramAlertFormatter())
    return handler

