import json
import logging
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHORT_HORIZON_ROOT = REPO_ROOT / "v2" / "short_horizon"
if str(SHORT_HORIZON_ROOT) not in sys.path:
    sys.path.insert(0, str(SHORT_HORIZON_ROOT))

from short_horizon.telemetry.alerting import create_telegram_alert_handler

class TestAlertFilter(unittest.TestCase):
    def test_formatter_filters_uninteresting_events(self) -> None:
        handler = create_telegram_alert_handler("token", "123")
        
        # Boring info event
        msg_json = json.dumps({"event": "book_update_ingested", "level": "info"})
        record = logging.LogRecord("test", logging.INFO, "path", 1, msg_json, None, None)
        self.assertEqual(handler.formatter.format(record), "")

        # Interesting info event
        msg_json = json.dumps({"event": "live_order_accepted", "level": "info"})
        record = logging.LogRecord("test", logging.INFO, "path", 1, msg_json, None, None)
        self.assertIn("live_order_accepted", handler.formatter.format(record))
        
        # Boring error event (all errors should pass)
        msg_json = json.dumps({"event": "some_random_error", "level": "error"})
        record = logging.LogRecord("test", logging.ERROR, "path", 1, msg_json, None, None)
        self.assertIn("some_random_error", handler.formatter.format(record))

if __name__ == "__main__":
    unittest.main()
