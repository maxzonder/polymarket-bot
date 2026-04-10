from unittest.mock import patch

from scripts.honest_replay_analyze import _resolve_tape_db_path


def test_resolve_tape_db_path_prefers_run_meta_path(tmp_path):
    explicit = tmp_path / "explicit_tape.db"
    explicit.write_text("x")
    fallback = tmp_path / "fallback_tape.db"
    fallback.write_text("y")

    with patch("scripts.honest_replay_analyze.DEFAULT_TAPE_DB_PATH", fallback):
        resolved = _resolve_tape_db_path({"tape_db_path": str(explicit)})

    assert resolved == explicit



def test_resolve_tape_db_path_falls_back_to_default_when_run_meta_missing(tmp_path):
    fallback = tmp_path / "fallback_tape.db"
    fallback.write_text("y")

    with patch("scripts.honest_replay_analyze.DEFAULT_TAPE_DB_PATH", fallback):
        resolved = _resolve_tape_db_path({})

    assert resolved == fallback
