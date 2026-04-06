from pathlib import Path

from scripts.run_honest_replay import _reset_replay_output


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
