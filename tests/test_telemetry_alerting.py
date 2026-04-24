import json
import logging
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.telemetry.alerting import TelegramWebhookHandler, create_telegram_alert_handler

class TestTelegramAlerting(unittest.TestCase):
    @patch("urllib.request.urlopen")
    def test_telegram_webhook_handler_emits_message(self, mock_urlopen) -> None:
        handler = TelegramWebhookHandler("fake_token", "fake_chat_id")
        record = logging.LogRecord("test", logging.INFO, "path", 1, "test message", None, None)
        
        mock_response = MagicMock()
        mock_urlopen.return_value.__enter__.return_value = mock_response
        
        handler.emit(record)
        
        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.full_url, "https://api.telegram.org/botfake_token/sendMessage")
        self.assertEqual(req.headers["Content-type"], "application/json")
        
        payload = json.loads(req.data.decode("utf-8"))
        self.assertEqual(payload["chat_id"], "fake_chat_id")
        self.assertEqual(payload["text"], "test message")
        self.assertEqual(payload["parse_mode"], "HTML")

    def test_create_telegram_alert_handler_returns_none_if_missing_creds(self) -> None:
        self.assertIsNone(create_telegram_alert_handler(None, "123"))
        self.assertIsNone(create_telegram_alert_handler("token", None))
        self.assertIsNone(create_telegram_alert_handler("", ""))

    def test_telegram_alert_formatter_formats_structlog_json(self) -> None:
        handler = create_telegram_alert_handler("token", "123")
        self.assertIsNotNone(handler)
        
        # Simulate structlog JSON output
        msg_json = json.dumps({
            "event": "live_order_accepted",
            "level": "info",
            "order_id": "m1:tok:0.55:100",
            "market_id": "m1",
            "size": 1.5,
            "source": "some_source"
        })
        
        record = logging.LogRecord("test", logging.INFO, "path", 1, msg_json, None, None)
        formatted = handler.formatter.format(record)
        
        self.assertIn("<b>[INFO] live_order_accepted</b>", formatted)
        self.assertIn("• <b>order_id</b>: m1:tok:0.55:100", formatted)
        self.assertIn("• <b>market_id</b>: m1", formatted)
        self.assertIn("• <b>size</b>: 1.5", formatted)
        self.assertNotIn("source", formatted) # Should be skipped by the formatter

if __name__ == "__main__":
    unittest.main()
