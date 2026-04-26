#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import random
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

DEFAULT_TOUCH_DATASET_PATH = Path("v2/short_horizon/data/touch_dataset.sqlite3")
DEFAULT_SPOT_FEATURES_PATH = Path("v2/short_horizon/data/spot_features.sqlite3")
DEFAULT_ARTIFACT_PATH = Path("v2/short_horizon/data/touch_model_artifact.json")
DEFAULT_SCORES_PATH = Path("v2/short_horizon/data/touch_model_scores.sqlite3")
DEFAULT_REPORT_PATH = Path("v2/short_horizon/docs/phase6/p6-2c_eval_report.md")
DEFAULT_SEED = 2602

FEATURE_NAMES = [
    "bid_ask_spread_at_touch",
    "book_imbalance_top1",
    "book_depth_top5",
    "time_since_last_trade_ms",
    "recent_trade_arrival_rate",
    "recent_realized_vol",
    "touch_velocity",
    "level",
    "price_at_touch",
    "lifecycle_fraction",
    "seconds_to_resolution",
    "spot_return_since_window_start",
    "spot_realized_vol_recent_60s",
    "spot_realized_vol_recent_300s",
    "spot_velocity_recent_30s",
    "spot_implied_prob_minus_market_prob",
    "asset_btc",
    "asset_eth",
    "direction_up",
]

OPTIONAL_ZERO_FEATURES = {
    "time_since_last_trade_ms",
    "recent_trade_arrival_rate",
    "recent_realized_vol",
    "touch_velocity",
}


@dataclass(frozen=True)
class TouchModelRow:
    probe_id: str
    market_id: str
    touch_time_iso: str
    event_time_ms: int
    asset_slug: str
    direction: str
    resolves_yes: int
    market_price: float
    fee_rate_bps: float
    would_be_cost_after_min_shares: float | None
    survived_ms: float | None
    features: dict[str, float]


@dataclass(frozen=True)
class LogisticArtifact:
    artifact_type: str
    model_type: str
    created_at: str
    seed: int
    feature_names: list[str]
    means: list[float]
    stds: list[float]
    weights: list[float]
    intercept: float
    train_fraction: float
    split_event_time_ms: int | None
    train_rows: int
    test_rows: int
    metrics: dict[str, Any]

    def predict_features(self, features: dict[str, float]) -> float:
        x = [_safe_float(features.get(name), 0.0) for name in self.feature_names]
        z = self.intercept
        for value, mean, std, weight in zip(x, self.means, self.stds, self.weights):
            z += ((value - mean) / std) * weight
        return _sigmoid(z)


@dataclass(frozen=True)
class TrainSummary:
    touch_dataset_path: str
    spot_features_path: str
    artifact_path: str
    scores_path: str
    report_path: str
    joined_rows: int
    train_rows: int
    test_rows: int
    metrics: dict[str, Any]

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


def train_touch_model(
    touch_dataset_path: str | Path = DEFAULT_TOUCH_DATASET_PATH,
    spot_features_path: str | Path = DEFAULT_SPOT_FEATURES_PATH,
    *,
    artifact_path: str | Path = DEFAULT_ARTIFACT_PATH,
    scores_path: str | Path = DEFAULT_SCORES_PATH,
    report_path: str | Path = DEFAULT_REPORT_PATH,
    train_fraction: float = 0.60,
    seed: int = DEFAULT_SEED,
    learning_rate: float = 0.05,
    epochs: int = 1200,
    l2: float = 0.001,
) -> TrainSummary:
    rows = load_joined_rows(touch_dataset_path, spot_features_path)
    train_rows, test_rows = walk_forward_split(rows, train_fraction=train_fraction)
    if not train_rows:
        raise ValueError("walk-forward split produced no training rows")
    if not test_rows:
        raise ValueError("walk-forward split produced no held-out test rows")

    artifact = fit_logistic_artifact(
        train_rows,
        test_rows,
        train_fraction=train_fraction,
        seed=seed,
        learning_rate=learning_rate,
        epochs=epochs,
        l2=l2,
    )
    train_probe_ids = {row.probe_id for row in train_rows}
    predictions = [(row, artifact.predict_features(row.features), "train" if row.probe_id in train_probe_ids else "test") for row in rows]

    artifact_path = Path(artifact_path)
    scores_path = Path(scores_path)
    report_path = Path(report_path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    scores_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    artifact_path.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_scores(scores_path, predictions, artifact.feature_names)
    report_path.write_text(render_report(artifact, rows, predictions, artifact_path, scores_path), encoding="utf-8")

    return TrainSummary(
        touch_dataset_path=str(touch_dataset_path),
        spot_features_path=str(spot_features_path),
        artifact_path=str(artifact_path),
        scores_path=str(scores_path),
        report_path=str(report_path),
        joined_rows=len(rows),
        train_rows=len(train_rows),
        test_rows=len(test_rows),
        metrics=artifact.metrics,
    )


def load_joined_rows(touch_dataset_path: str | Path, spot_features_path: str | Path) -> list[TouchModelRow]:
    touch_dataset_path = Path(touch_dataset_path)
    spot_features_path = Path(spot_features_path)
    touch_conn = sqlite3.connect(touch_dataset_path)
    touch_conn.row_factory = sqlite3.Row
    spot_conn = sqlite3.connect(spot_features_path)
    spot_conn.row_factory = sqlite3.Row
    try:
        _require_table(touch_conn, "touch_dataset", touch_dataset_path)
        _require_table(spot_conn, "spot_features", spot_features_path)
        touch_cols = _table_columns(touch_conn, "touch_dataset")
        spot_cols = _table_columns(spot_conn, "spot_features")
        required_touch = {"probe_id", "touch_time_iso", "market_id", "asset_slug", "direction", "resolves_yes", "best_ask_at_touch"}
        missing_touch = sorted(required_touch - touch_cols)
        if missing_touch:
            raise ValueError(f"touch_dataset missing required columns: {missing_touch}")
        required_spot = {"probe_id", "spot_return_since_window_start", "spot_velocity_recent_30s", "spot_implied_prob_minus_market_prob"}
        missing_spot = sorted(required_spot - spot_cols)
        if missing_spot:
            raise ValueError(f"spot_features missing required columns: {missing_spot}")

        spot_by_probe = {str(row["probe_id"]): dict(row) for row in spot_conn.execute("SELECT * FROM spot_features")}
        rows: list[TouchModelRow] = []
        for touch in touch_conn.execute("SELECT * FROM touch_dataset ORDER BY touch_time_iso, probe_id"):
            probe_id = str(touch["probe_id"])
            spot = spot_by_probe.get(probe_id)
            if spot is None:
                continue
            touch_dict = dict(touch)
            features = build_feature_row(touch_dict, spot)
            price = _safe_float(touch_dict.get("best_ask_at_touch"), 0.0)
            if price <= 0:
                continue
            touch_time_iso = str(touch_dict.get("touch_time_iso") or "")
            rows.append(
                TouchModelRow(
                    probe_id=probe_id,
                    market_id=str(touch_dict.get("market_id") or ""),
                    touch_time_iso=touch_time_iso,
                    event_time_ms=parse_iso_ms(touch_time_iso),
                    asset_slug=str(touch_dict.get("asset_slug") or "").lower(),
                    direction=str(touch_dict.get("direction") or ""),
                    resolves_yes=int(_safe_float(touch_dict.get("resolves_yes"), 0.0)),
                    market_price=price,
                    fee_rate_bps=_safe_float(touch_dict.get("fee_rate_bps"), 0.0),
                    would_be_cost_after_min_shares=_optional_float(touch_dict.get("would_be_cost_after_min_shares")),
                    survived_ms=_optional_float(touch_dict.get("survived_ms")),
                    features=features,
                )
            )
        if not rows:
            raise ValueError("joined touch_dataset × spot_features has zero usable rows")
        return rows
    finally:
        touch_conn.close()
        spot_conn.close()


def build_feature_row(touch: dict[str, Any], spot: dict[str, Any]) -> dict[str, float]:
    ask = _safe_float(touch.get("best_ask_at_touch"), 0.0)
    bid = _safe_float(touch.get("best_bid_at_touch"), ask)
    ask_sizes = [_safe_float(touch.get(f"ask_level_{idx}_size"), 0.0) for idx in range(1, 6)]
    top5 = sum(size for size in ask_sizes if size > 0)
    direction = str(touch.get("direction") or "").upper()
    asset = str(touch.get("asset_slug") or "").lower()
    seconds_to_resolution = _first_float(
        spot.get("seconds_to_resolution"),
        _seconds_between(touch.get("touch_time_iso"), touch.get("end_time_iso")),
        0.0,
    )
    features = {
        "bid_ask_spread_at_touch": max(0.0, ask - bid),
        "book_imbalance_top1": (ask_sizes[0] / top5) if top5 > 0 else 0.0,
        "book_depth_top5": top5,
        "time_since_last_trade_ms": _safe_float(touch.get("time_since_last_trade_ms"), 0.0),
        "recent_trade_arrival_rate": _safe_float(touch.get("recent_trade_arrival_rate"), 0.0),
        "recent_realized_vol": _first_float(touch.get("recent_realized_vol"), spot.get("spot_realized_vol_recent_60s"), 0.0),
        "touch_velocity": _safe_float(touch.get("touch_velocity"), 0.0),
        "level": _safe_float(touch.get("touch_level"), ask),
        "price_at_touch": ask,
        "lifecycle_fraction": _safe_float(touch.get("lifecycle_fraction"), 0.0),
        "seconds_to_resolution": seconds_to_resolution,
        "spot_return_since_window_start": _safe_float(spot.get("spot_return_since_window_start"), 0.0),
        "spot_realized_vol_recent_60s": _safe_float(spot.get("spot_realized_vol_recent_60s"), 0.0),
        "spot_realized_vol_recent_300s": _safe_float(spot.get("spot_realized_vol_recent_300s"), 0.0),
        "spot_velocity_recent_30s": _safe_float(spot.get("spot_velocity_recent_30s"), 0.0),
        "spot_implied_prob_minus_market_prob": _safe_float(spot.get("spot_implied_prob_minus_market_prob"), 0.0),
        "asset_btc": 1.0 if asset in {"btc", "bitcoin"} else 0.0,
        "asset_eth": 1.0 if asset in {"eth", "ethereum"} else 0.0,
        "direction_up": 1.0 if direction.startswith("UP") or direction.endswith("YES") else 0.0,
    }
    return {name: _safe_float(features.get(name), 0.0) for name in FEATURE_NAMES}


def walk_forward_split(rows: Sequence[TouchModelRow], *, train_fraction: float = 0.60) -> tuple[list[TouchModelRow], list[TouchModelRow]]:
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")
    ordered = sorted(rows, key=lambda row: (row.event_time_ms, row.probe_id))
    split_idx = max(1, min(len(ordered) - 1, int(len(ordered) * train_fraction)))
    return ordered[:split_idx], ordered[split_idx:]


def fit_logistic_artifact(
    train_rows: Sequence[TouchModelRow],
    test_rows: Sequence[TouchModelRow],
    *,
    train_fraction: float,
    seed: int,
    learning_rate: float,
    epochs: int,
    l2: float,
) -> LogisticArtifact:
    random.seed(seed)
    train_x = [[row.features[name] for name in FEATURE_NAMES] for row in train_rows]
    train_y = [row.resolves_yes for row in train_rows]
    test_x = [[row.features[name] for name in FEATURE_NAMES] for row in test_rows]
    test_y = [row.resolves_yes for row in test_rows]
    means, stds = _fit_standardizer(train_x)
    train_z = [_standardize(row, means, stds) for row in train_x]
    test_z = [_standardize(row, means, stds) for row in test_x]
    weights, intercept = _fit_logistic(train_z, train_y, learning_rate=learning_rate, epochs=epochs, l2=l2)
    train_prob = [_predict_standardized(row, weights, intercept) for row in train_z]
    test_prob = [_predict_standardized(row, weights, intercept) for row in test_z]
    split_time = min(row.event_time_ms for row in test_rows) if test_rows else None
    metrics = {
        "train_auc": auc_score(train_y, train_prob),
        "test_auc": auc_score(test_y, test_prob),
        "train_log_loss": log_loss(train_y, train_prob),
        "test_log_loss": log_loss(test_y, test_prob),
        "train_positive_rate": sum(train_y) / len(train_y) if train_y else None,
        "test_positive_rate": sum(test_y) / len(test_y) if test_y else None,
        "calibration_deciles_test": calibration_buckets(test_y, test_prob, buckets=10),
        "policy_ev_deciles_test": policy_ev_deciles(test_rows, test_prob, buckets=10),
    }
    return LogisticArtifact(
        artifact_type="p6_2c_touch_model",
        model_type="logistic_regression_stdlib",
        created_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        seed=seed,
        feature_names=list(FEATURE_NAMES),
        means=means,
        stds=stds,
        weights=weights,
        intercept=intercept,
        train_fraction=train_fraction,
        split_event_time_ms=split_time,
        train_rows=len(train_rows),
        test_rows=len(test_rows),
        metrics=metrics,
    )


def load_artifact(path: str | Path) -> LogisticArtifact:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return LogisticArtifact(**payload)


def _fit_standardizer(matrix: Sequence[Sequence[float]]) -> tuple[list[float], list[float]]:
    n_features = len(matrix[0]) if matrix else 0
    means: list[float] = []
    stds: list[float] = []
    for col in range(n_features):
        values = [row[col] for row in matrix]
        mean = sum(values) / len(values)
        var = sum((value - mean) ** 2 for value in values) / len(values)
        std = math.sqrt(var) or 1.0
        means.append(mean)
        stds.append(std)
    return means, stds


def _standardize(row: Sequence[float], means: Sequence[float], stds: Sequence[float]) -> list[float]:
    return [(value - mean) / std for value, mean, std in zip(row, means, stds)]


def _fit_logistic(
    matrix: Sequence[Sequence[float]],
    labels: Sequence[int],
    *,
    learning_rate: float,
    epochs: int,
    l2: float,
) -> tuple[list[float], float]:
    if not matrix:
        raise ValueError("empty training matrix")
    n_rows = len(matrix)
    n_features = len(matrix[0])
    weights = [0.0] * n_features
    positive_rate = min(0.999, max(0.001, sum(labels) / len(labels)))
    intercept = math.log(positive_rate / (1.0 - positive_rate))
    for _ in range(epochs):
        grad_w = [0.0] * n_features
        grad_b = 0.0
        for x, y in zip(matrix, labels):
            pred = _predict_standardized(x, weights, intercept)
            err = pred - y
            grad_b += err
            for idx, value in enumerate(x):
                grad_w[idx] += err * value
        intercept -= learning_rate * grad_b / n_rows
        for idx in range(n_features):
            grad = (grad_w[idx] / n_rows) + (l2 * weights[idx])
            weights[idx] -= learning_rate * grad
    return weights, intercept


def _predict_standardized(row: Sequence[float], weights: Sequence[float], intercept: float) -> float:
    return _sigmoid(intercept + sum(value * weight for value, weight in zip(row, weights)))


def auc_score(labels: Sequence[int], probs: Sequence[float]) -> float | None:
    pairs = sorted(zip(probs, labels), key=lambda item: item[0])
    positives = sum(1 for _, label in pairs if label == 1)
    negatives = len(pairs) - positives
    if positives == 0 or negatives == 0:
        return None
    rank_sum = 0.0
    rank = 1
    idx = 0
    while idx < len(pairs):
        end = idx + 1
        while end < len(pairs) and pairs[end][0] == pairs[idx][0]:
            end += 1
        avg_rank = (rank + rank + (end - idx) - 1) / 2.0
        for j in range(idx, end):
            if pairs[j][1] == 1:
                rank_sum += avg_rank
        rank += end - idx
        idx = end
    return (rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives)


def log_loss(labels: Sequence[int], probs: Sequence[float]) -> float | None:
    if not labels:
        return None
    total = 0.0
    for y, p in zip(labels, probs):
        p = min(1.0 - 1e-12, max(1e-12, p))
        total += -(y * math.log(p) + (1 - y) * math.log(1 - p))
    return total / len(labels)


def calibration_buckets(labels: Sequence[int], probs: Sequence[float], *, buckets: int = 10) -> list[dict[str, Any]]:
    ordered = sorted(zip(probs, labels), key=lambda item: item[0])
    out: list[dict[str, Any]] = []
    for idx, chunk in enumerate(_chunks_by_rank(ordered, buckets), start=1):
        if not chunk:
            continue
        ps = [p for p, _ in chunk]
        ys = [y for _, y in chunk]
        out.append({"bucket": idx, "rows": len(chunk), "avg_pred": sum(ps) / len(ps), "actual_rate": sum(ys) / len(ys)})
    return out


def policy_ev_deciles(rows: Sequence[TouchModelRow], probs: Sequence[float], *, buckets: int = 10) -> list[dict[str, Any]]:
    ordered = sorted(zip(probs, rows), key=lambda item: item[0])
    out: list[dict[str, Any]] = []
    for idx, chunk in enumerate(_chunks_by_rank(ordered, buckets), start=1):
        if not chunk:
            continue
        stakes = []
        pnls = []
        for prob, row in chunk:
            stake = max(0.01, row.would_be_cost_after_min_shares or row.market_price * 5.0)
            fee = stake * row.fee_rate_bps / 10000.0
            pnl = ((stake / row.market_price) if row.resolves_yes else 0.0) - stake - fee
            stakes.append(stake)
            pnls.append(pnl)
        out.append(
            {
                "decile": idx,
                "rows": len(chunk),
                "avg_model_prob": sum(prob for prob, _ in chunk) / len(chunk),
                "avg_market_price": sum(row.market_price for _, row in chunk) / len(chunk),
                "hit_rate": sum(row.resolves_yes for _, row in chunk) / len(chunk),
                "net_ev_per_usdc": (sum(pnls) / sum(stakes)) if stakes and sum(stakes) else None,
            }
        )
    return out


def write_scores(path: str | Path, predictions: Sequence[tuple[TouchModelRow, float, str]], feature_names: Sequence[str]) -> None:
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE IF EXISTS touch_model_scores")
    conn.execute(
        """
        CREATE TABLE touch_model_scores (
            probe_id TEXT PRIMARY KEY,
            split TEXT NOT NULL,
            market_id TEXT,
            touch_time_iso TEXT NOT NULL,
            event_time_ms INTEGER NOT NULL,
            asset_slug TEXT,
            direction TEXT,
            resolves_yes INTEGER NOT NULL,
            market_price REAL NOT NULL,
            fee_rate_bps REAL NOT NULL,
            would_be_cost_after_min_shares REAL,
            survived_ms REAL,
            model_prob REAL NOT NULL
        )
        """
    )
    for row, prob, split in predictions:
        conn.execute(
            """
            INSERT INTO touch_model_scores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.probe_id,
                split,
                row.market_id,
                row.touch_time_iso,
                row.event_time_ms,
                row.asset_slug,
                row.direction,
                row.resolves_yes,
                row.market_price,
                row.fee_rate_bps,
                row.would_be_cost_after_min_shares,
                row.survived_ms,
                prob,
            ),
        )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_model_scores_split_prob ON touch_model_scores(split, model_prob)")
    conn.commit()
    conn.close()


def render_report(
    artifact: LogisticArtifact,
    rows: Sequence[TouchModelRow],
    predictions: Sequence[tuple[TouchModelRow, float, str]],
    artifact_path: Path,
    scores_path: Path,
) -> str:
    test = [(row, prob) for row, prob, split in predictions if split == "test"]
    top_features = sorted(zip(artifact.feature_names, artifact.weights), key=lambda item: abs(item[1]), reverse=True)[:12]
    lines = [
        "# P6-2c offline touch model evaluation",
        "",
        f"Generated: `{artifact.created_at}`",
        "",
        "## Inputs / artifacts",
        "",
        f"- Joined rows: `{len(rows)}`",
        f"- Train rows: `{artifact.train_rows}` earliest rows (`{artifact.train_fraction:.0%}` walk-forward split)",
        f"- Held-out rows: `{artifact.test_rows}` latest rows",
        f"- Model artifact: `{artifact_path}`",
        f"- Scores DB: `{scores_path}`",
        "- Model: stdlib logistic regression with fixed seed; no random row shuffle.",
        "",
        "## Diagnostics",
        "",
        f"- Train AUC: `{_fmt(artifact.metrics.get('train_auc'))}`",
        f"- Held-out AUC: `{_fmt(artifact.metrics.get('test_auc'))}`",
        f"- Train log-loss: `{_fmt(artifact.metrics.get('train_log_loss'))}`",
        f"- Held-out log-loss: `{_fmt(artifact.metrics.get('test_log_loss'))}`",
        f"- Train positive rate: `{_fmt(artifact.metrics.get('train_positive_rate'))}`",
        f"- Held-out positive rate: `{_fmt(artifact.metrics.get('test_positive_rate'))}`",
        "",
        "## Top logistic coefficients",
        "",
    ]
    for name, weight in top_features:
        lines.append(f"- `{name}`: `{weight:.6f}`")
    lines.extend(["", "## Held-out calibration by model-probability decile", ""])
    for bucket in artifact.metrics.get("calibration_deciles_test") or []:
        lines.append(
            f"- Bucket {bucket['bucket']}: rows `{bucket['rows']}`, avg_pred `{bucket['avg_pred']:.4f}`, actual `{bucket['actual_rate']:.4f}`"
        )
    lines.extend(["", "## Held-out policy EV by model-probability decile", ""])
    for bucket in artifact.metrics.get("policy_ev_deciles_test") or []:
        lines.append(
            f"- Decile {bucket['decile']}: rows `{bucket['rows']}`, avg_model_prob `{bucket['avg_model_prob']:.4f}`, "
            f"hit_rate `{bucket['hit_rate']:.4f}`, net_ev_per_usdc `{_fmt(bucket.get('net_ev_per_usdc'))}`"
        )
    lines.extend(
        [
            "",
            "## GO/NO-GO",
            "",
            "This file is the model diagnostic report. The trading GO gate is decided by `scripts/evaluate_touch_policy.py`, which applies fees, min-size, edge buffer, stale-book guard, market/order limits, daily-loss cap, and bootstrap CI on held-out rows.",
            "",
        ]
    )
    return "\n".join(lines)


def _chunks_by_rank(items: Sequence[Any], buckets: int) -> list[list[Any]]:
    if not items:
        return []
    out: list[list[Any]] = []
    n = len(items)
    for bucket in range(buckets):
        start = bucket * n // buckets
        end = (bucket + 1) * n // buckets
        out.append(list(items[start:end]))
    return out


def parse_iso_ms(value: str) -> int:
    if not value:
        return 0
    normalized = value.replace("Z", "+00:00")
    return int(datetime.fromisoformat(normalized).timestamp() * 1000)


def _seconds_between(start: Any, end: Any) -> float | None:
    if not start or not end:
        return None
    try:
        return max(0.0, (parse_iso_ms(str(end)) - parse_iso_ms(str(start))) / 1000.0)
    except ValueError:
        return None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _require_table(conn: sqlite3.Connection, table: str, path: str | Path) -> None:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is None:
        raise ValueError(f"{path} does not contain table {table}")


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _first_float(*values: Any) -> float:
    for value in values:
        parsed = _optional_float(value)
        if parsed is not None:
            return parsed
    return 0.0


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Train P6-2c offline touch model with walk-forward split")
    parser.add_argument("--touch-dataset", default=str(DEFAULT_TOUCH_DATASET_PATH), help="SQLite touch_dataset path from P6-2a")
    parser.add_argument("--spot-features", default=str(DEFAULT_SPOT_FEATURES_PATH), help="SQLite spot_features path from P6-2b")
    parser.add_argument("--artifact", default=str(DEFAULT_ARTIFACT_PATH), help="Output JSON model artifact")
    parser.add_argument("--scores", default=str(DEFAULT_SCORES_PATH), help="Output SQLite scores DB")
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH), help="Output markdown diagnostics report")
    parser.add_argument("--train-fraction", type=float, default=0.60, help="Earliest fraction used for training; remainder is held out")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--epochs", type=int, default=1200)
    parser.add_argument("--l2", type=float, default=0.001)
    args = parser.parse_args()
    summary = train_touch_model(
        args.touch_dataset,
        args.spot_features,
        artifact_path=args.artifact,
        scores_path=args.scores,
        report_path=args.report,
        train_fraction=args.train_fraction,
        seed=args.seed,
        learning_rate=args.learning_rate,
        epochs=args.epochs,
        l2=args.l2,
    )
    print(json.dumps(summary.as_json(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
