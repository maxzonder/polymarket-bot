from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class BinRunnerSmokeTest(unittest.TestCase):
    def _run_help(self, relative_path: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, relative_path, "--help"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_live_runner_help(self) -> None:
        result = self._run_help("v2/short_horizon/bin/live_runner")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("live", result.stdout.lower())
        self.assertIn("stub", result.stdout.lower())

    def test_replay_runner_help(self) -> None:
        result = self._run_help("v2/short_horizon/bin/replay_runner")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("replay", result.stdout.lower())
        self.assertIn("event_log_path", result.stdout)


if __name__ == "__main__":
    unittest.main()
