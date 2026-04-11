#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class RunResult:
    config_name: str
    config_path: str
    run_dir: str
    dryrun_log: str
    analyzer_log: str | None
    html_report: str | None
    started_at_utc: str
    finished_at_utc: str | None
    dryrun_returncode: int | None
    analyzer_returncode: int | None
    status: str
    error: str | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_logged(cmd: list[str], *, cwd: Path, log_path: Path, env: dict[str, str]) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as fh:
        fh.write(f"started_at_utc: {_utc_now()}\n")
        fh.write(f"cwd: {cwd}\n")
        fh.write("command: " + " ".join(cmd) + "\n\n")
        fh.flush()
        proc = subprocess.run(cmd, cwd=cwd, env=env, stdout=fh, stderr=subprocess.STDOUT, text=True)
        fh.write(f"\nfinished_at_utc: {_utc_now()}\n")
        fh.write(f"returncode: {proc.returncode}\n")
        return proc.returncode


def _clear_config_pyc(repo_root: Path) -> None:
    pycache = repo_root / "__pycache__"
    if not pycache.exists():
        return
    for path in pycache.glob("config.cpython-*.pyc"):
        path.unlink(missing_ok=True)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_status(path: Path, *, total: int, completed: int, current: str | None, state: str, output_root: Path) -> None:
    lines = [
        f"updated_at_utc: {_utc_now()}",
        f"state: {state}",
        f"completed: {completed}/{total}",
        f"current: {current or '-'}",
        f"output_root: {output_root}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a tape dryrun matrix by swapping config.py variants one-by-one")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--mode", default="big_swan_mode")
    parser.add_argument("--configs-dir", default="replay_matrix_configs")
    parser.add_argument("--config-glob", default="full_bs_bal_*_rest_*.py")
    parser.add_argument("--output-root", default=None, help="Matrix root dir, e.g. /home/polybot/.polybot/dryruns/matrix_mar15_mar31_...")
    parser.add_argument("--limit-markets", type=int, default=None)
    parser.add_argument("--batch-seconds", type=int, default=None)
    parser.add_argument("--tape-db", default=None)
    parser.add_argument("--analyzer-top", type=int, default=20)
    parser.add_argument("--stop-on-error", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config.py"
    configs_dir = (repo_root / args.configs_dir).resolve()
    config_paths = sorted(configs_dir.glob(args.config_glob))
    if not config_paths:
        raise SystemExit(f"No config files matched: {configs_dir / args.config_glob}")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output_root).resolve() if args.output_root else (repo_root / "out" / f"tape_matrix_{args.start}_{args.end}_{stamp}")
    output_root.mkdir(parents=True, exist_ok=True)
    status_path = output_root / "STATUS.txt"
    summary_path = output_root / "matrix_summary.json"
    env = os.environ.copy()

    original_config = config_path.read_bytes()
    results: list[RunResult] = []

    _write_status(status_path, total=len(config_paths), completed=0, current=None, state="starting", output_root=output_root)
    _write_json(
        output_root / "matrix_manifest.json",
        {
            "created_at_utc": _utc_now(),
            "repo_root": str(repo_root),
            "start": args.start,
            "end": args.end,
            "mode": args.mode,
            "configs_dir": str(configs_dir),
            "config_glob": args.config_glob,
            "configs": [str(p) for p in config_paths],
            "output_root": str(output_root),
            "analyzer_top": args.analyzer_top,
            "limit_markets": args.limit_markets,
            "batch_seconds": args.batch_seconds,
            "tape_db": args.tape_db,
        },
    )

    try:
        for idx, variant_path in enumerate(config_paths, start=1):
            name = variant_path.stem
            run_dir = output_root / name
            run_dir.mkdir(parents=True, exist_ok=True)
            started_at = _utc_now()
            dryrun_log = run_dir / f"run_tape_dryrun_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
            analyzer_log = run_dir / f"honest_replay_analyze_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
            html_report = run_dir / "analyzer_report_current.html"

            _write_status(status_path, total=len(config_paths), completed=idx - 1, current=name, state=f"running {idx}/{len(config_paths)}", output_root=output_root)
            shutil.copy2(variant_path, config_path)
            _clear_config_pyc(repo_root)

            meta = {
                "config_name": name,
                "config_path": str(variant_path),
                "index": idx,
                "total": len(config_paths),
                "started_at_utc": started_at,
                "run_dir": str(run_dir),
            }
            _write_json(run_dir / "matrix_run_meta.json", meta)

            dryrun_cmd = [
                sys.executable,
                "-u",
                "scripts/run_tape_dryrun.py",
                "--start",
                args.start,
                "--end",
                args.end,
                "--mode",
                args.mode,
                "--out",
                str(run_dir),
            ]
            if args.limit_markets is not None:
                dryrun_cmd.extend(["--limit-markets", str(args.limit_markets)])
            if args.batch_seconds is not None:
                dryrun_cmd.extend(["--batch-seconds", str(args.batch_seconds)])
            if args.tape_db:
                dryrun_cmd.extend(["--tape-db", args.tape_db])

            dryrun_rc = _run_logged(dryrun_cmd, cwd=repo_root, log_path=dryrun_log, env=env)
            analyzer_rc: int | None = None
            status = "dryrun_failed" if dryrun_rc != 0 else "dryrun_ok"
            error: str | None = None

            if dryrun_rc == 0:
                analyzer_cmd = [
                    sys.executable,
                    "-u",
                    "scripts/honest_replay_analyze.py",
                    "--run-dir",
                    str(run_dir),
                    "--top",
                    str(args.analyzer_top),
                ]
                analyzer_rc = _run_logged(analyzer_cmd, cwd=repo_root, log_path=analyzer_log, env=env)
                status = "ok" if analyzer_rc == 0 else "analyzer_failed"
                if analyzer_rc != 0:
                    error = f"analyzer failed with code {analyzer_rc}"
            else:
                analyzer_log = None
                html_report = None
                error = f"dryrun failed with code {dryrun_rc}"

            result = RunResult(
                config_name=name,
                config_path=str(variant_path),
                run_dir=str(run_dir),
                dryrun_log=str(dryrun_log),
                analyzer_log=str(analyzer_log) if analyzer_log else None,
                html_report=str(html_report) if html_report and html_report.exists() else None,
                started_at_utc=started_at,
                finished_at_utc=_utc_now(),
                dryrun_returncode=dryrun_rc,
                analyzer_returncode=analyzer_rc,
                status=status,
                error=error,
            )
            results.append(result)
            _write_json(summary_path, {
                "created_at_utc": _utc_now(),
                "start": args.start,
                "end": args.end,
                "output_root": str(output_root),
                "results": [asdict(item) for item in results],
            })

            if status != "ok" and args.stop_on_error:
                break

        ok_count = sum(1 for item in results if item.status == "ok")
        _write_status(status_path, total=len(config_paths), completed=len(results), current=None, state=f"finished ok={ok_count} total={len(results)}", output_root=output_root)
        return 0 if ok_count == len(config_paths) else 1
    finally:
        config_path.write_bytes(original_config)
        _clear_config_pyc(repo_root)


if __name__ == "__main__":
    raise SystemExit(main())
