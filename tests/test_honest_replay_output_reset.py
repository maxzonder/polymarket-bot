from pathlib import Path
from unittest.mock import patch

from replay.honest_replay import _history_run_dir, _reset_replay_output


def test_reset_replay_output_removes_stale_dbs_and_keeps_dir(tmp_path):
    out = tmp_path / "replay_run"
    out.mkdir(parents=True)
    positions = out / "positions.db"
    paper = out / "paper_trades.db"
    extra = out / "notes.txt"

    positions.write_text("stale positions")
    paper.write_text("stale paper")
    extra.write_text("keep me")

    got_positions, got_paper = _reset_replay_output(out)

    assert got_positions == positions
    assert got_paper == paper
    assert out.exists() and out.is_dir()
    assert not positions.exists()
    assert not paper.exists()
    assert extra.exists()



def test_history_run_dir_creates_timestamped_child(tmp_path):
    root = tmp_path / "history"

    with patch("replay.honest_replay.time.strftime", return_value="20260406_104800"):
        run_dir = _history_run_dir(root)

    assert run_dir == root / "run_20260406_104800"
    assert run_dir.exists() and run_dir.is_dir()
