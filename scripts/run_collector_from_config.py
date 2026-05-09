#!/usr/bin/env python3
"""Run the multilevel collector from a small JSON/YAML config file.

The config files in `configs/collector/*.yaml` intentionally use JSON-compatible
YAML so this runner does not require PyYAML. If PyYAML is installed, normal YAML
also works.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.paths import DATA_DIR


def load_config(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        payload = yaml.safe_load(text)
    except Exception:
        payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError(f"config {path} must contain an object")
    return payload


def build_run_id(config: dict[str, Any], override: str | None = None) -> str:
    explicit = override or config.get("run_id")
    if explicit:
        return str(explicit)
    prefix = str(config.get("run_id_prefix") or "collector")
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


def build_collector_command(config: dict[str, Any], run_id: str) -> tuple[list[str], dict[str, Path]]:
    output_dir = Path(config.get("output_dir") or DATA_DIR / "short_horizon" / "phase0")
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{run_id}.csv"
    sqlite_path = output_dir / f"{run_id}.sqlite3"
    cmd = [
        sys.executable,
        "scripts/measure_live_depth_and_survival.py",
        "--run-id",
        run_id,
        "--level-preset",
        str(config["level_preset"]),
        "--universe-mode",
        str(config["universe_mode"]),
        "--output-csv",
        str(csv_path),
        "--output-sqlite",
        str(sqlite_path),
    ]
    scalar_flags = {
        "min_duration_seconds": "--min-duration-seconds",
        "max_duration_seconds": "--max-duration-seconds",
        "max_seconds_until_start": "--max-seconds-until-start",
        "max_seconds_to_end": "--max-seconds-to-end",
        "duration_metric": "--duration-metric",
        "book_snapshot_interval_ms": "--book-snapshot-interval-ms",
        "stale_book_ms": "--stale-book-ms",
        "wide_spread_threshold": "--wide-spread-threshold",
        "max_discovery_rows": "--max-discovery-rows",
        "discovery_order": "--discovery-order",
        "log_level": "--log-level",
        "max_events": "--max-events",
        "spot_feed": "--spot-feed",
        "spot_poll_interval_sec": "--spot-poll-interval-sec",
        "min_volume_usdc": "--min-volume-usdc",
        "max_volume_usdc": "--max-volume-usdc",
    }
    for key, flag in scalar_flags.items():
        if key in config and config[key] is not None:
            cmd.extend([flag, str(config[key])])
    list_flags = {
        "asset_slug": "--asset-slug",
        "market_keyword": "--market-keyword",
        "exclude_market_keyword": "--exclude-market-keyword",
        "payoff_type": "--payoff-type",
        "spot_asset_slug": "--spot-asset-slug",
    }
    for key, flag in list_flags.items():
        values = config.get(key) or []
        if isinstance(values, str):
            values = [values]
        for value in values:
            cmd.extend([flag, str(value)])
    if config.get("no_require_recurrence"):
        cmd.append("--no-require-recurrence")
    if config.get("discovery_ascending"):
        cmd.append("--discovery-ascending")
    return cmd, {"csv": csv_path, "sqlite": sqlite_path, "output_dir": output_dir}


def build_report_commands(config: dict[str, Any], run_id: str, sqlite_path: Path, output_dir: Path) -> list[list[str]]:
    if not config.get("build_reports", True):
        return []
    resolutions = Path(config.get("resolutions_sqlite") or DATA_DIR / "short_horizon" / "data" / "market_resolutions.sqlite3")
    heatmap_json = output_dir / f"{run_id}_heatmap.json"
    post_touch_json = output_dir / f"{run_id}_post_touch.json"
    signal_json = output_dir / f"{run_id}_signal_report.json"
    reports: list[list[str]] = []
    reports.append(
        [
            sys.executable,
            "scripts/build_collector_heatmap.py",
            "--input-sqlite",
            str(sqlite_path),
            "--output-md",
            str(output_dir / f"{run_id}_heatmap.md"),
            "--output-json",
            str(heatmap_json),
            "--min-rows",
            str(config.get("report_min_rows", 5)),
        ]
    )
    reports.append(
        [
            sys.executable,
            "scripts/enrich_collector_post_touch.py",
            "--input-sqlite",
            str(sqlite_path),
            "--output-md",
            str(output_dir / f"{run_id}_post_touch.md"),
            "--output-json",
            str(post_touch_json),
            "--min-rows",
            str(config.get("report_min_rows", 5)),
        ]
    )
    signal_axes = config.get("signal_axes") or ["duration_seconds", "asset_slug", "outcome"]
    signal = [
        sys.executable,
        "scripts/build_collector_signal_report.py",
        "--input-sqlite",
        str(sqlite_path),
        "--resolutions-sqlite",
        str(resolutions),
        "--output-md",
        str(output_dir / f"{run_id}_signal_report.md"),
        "--output-json",
        str(signal_json),
        "--min-rows",
        str(config.get("signal_min_rows", 10)),
        "--max-spread",
        str(config.get("signal_max_spread", 0.10)),
        "--backfill-resolutions",
    ]
    for axis in signal_axes:
        signal.extend(["--axis", str(axis)])
    reports.append(signal)
    if config.get("build_summary", True):
        reports.append(
            [
                sys.executable,
                "scripts/build_collector_run_summary.py",
                "--input-sqlite",
                str(sqlite_path),
                "--signal-json",
                str(signal_json),
                "--output-md",
                str(output_dir / f"{run_id}_summary.md"),
                "--output-json",
                str(output_dir / f"{run_id}_summary.json"),
            ]
        )
    return reports


def run_command(cmd: list[str], *, dry_run: bool) -> int:
    print("$ " + shlex.join(cmd), flush=True)
    if dry_run:
        return 0
    return subprocess.call(cmd)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--run-id", default=None, help="Override config run_id/run_id_prefix timestamp generation")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    config = load_config(args.config)
    run_id = build_run_id(config, args.run_id)
    cmd, paths = build_collector_command(config, run_id)
    timeout_seconds = config.get("timeout_seconds")
    if timeout_seconds:
        cmd = ["timeout", str(timeout_seconds), *cmd]
    code = run_command(cmd, dry_run=args.dry_run)
    if code not in (0, 124):
        raise SystemExit(code)
    for report in build_report_commands(config, run_id, paths["sqlite"], paths["output_dir"]):
        report_code = run_command(report, dry_run=args.dry_run)
        if report_code != 0:
            raise SystemExit(report_code)
    print(json.dumps({"run_id": run_id, "exit_code": code, "paths": {k: str(v) for k, v in paths.items()}}, sort_keys=True))


if __name__ == "__main__":
    main()
