from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Callable

from .config import RiskConfig, ShortHorizonConfig
from .core.runtime import StrategyRuntime
from .execution import ExecutionEngine, ExecutionMode, LiveSubmitGuard, LiveSubmitGuardRejected
from .market_data import LiveEventSource, MarketDataSource
from .probe import assert_min_book_updates_per_minute, maybe_cross_validate_probe_against_collector, summarize_probe_db
from .runner import RunnerSummary, drive_runtime_event_stream, drive_runtime_events
from .storage import RunContext, SQLiteRuntimeStore
from .strategies import ShortHorizon15mTouchStrategy
from .telemetry import configure_logging, get_logger
from .venue_polymarket import PolymarketUserStream
from .venue_polymarket.execution_client import PRIVATE_KEY_ENV_VAR, PolymarketExecutionClient, PolygonUsdcBridgeResult


@dataclass(frozen=True)
class KillSwitchSummary:
    open_order_count: int
    canceled_count: int
    failed_count: int


@dataclass(frozen=True)
class AllowanceApprovalSummary:
    total_actions: int
    approved_count: int
    already_approved_count: int


@dataclass(frozen=True)
class PolygonUsdcBridgeSummary:
    source_amount_base_unit: int
    deposit_address: str
    wallet_transfer_tx_hash: str
    bridge_status: str
    bridge_tx_hash: str | None
    quoted_target_amount_base_unit: int | None
    collateral_delta_base_unit: int


@dataclass
class OperatorConfirmLiveOrderGuard:
    max_live_orders_total: int | None = None
    require_confirmation: bool = False
    input_func: Callable[[str], str] = input
    output_func: Callable[[str], None] = print

    def __post_init__(self) -> None:
        self._successful_submits = 0

    def __call__(self, intent, order_request, order_row) -> None:
        if self.max_live_orders_total is not None and self._successful_submits >= self.max_live_orders_total:
            raise LiveSubmitGuardRejected(
                f"live order cap reached for this run: {self.max_live_orders_total}",
                reason_code="LIVE_ORDER_LIMIT_REACHED",
            )

        attempt_number = self._successful_submits + 1
        if self.require_confirmation:
            self.output_func(
                format_live_order_confirmation(
                    intent=intent,
                    order_request=order_request,
                    order_row=order_row,
                    attempt_number=attempt_number,
                    max_live_orders_total=self.max_live_orders_total,
                )
            )
            response = str(self.input_func("Submit this live order? [y/N]: ")).strip().lower()
            if response != "y":
                raise LiveSubmitGuardRejected(
                    "operator declined live order submission",
                    reason_code="OPERATOR_DECLINED",
                )

    def record_submit_success(self, intent, order_request, order_row, place_result) -> None:
        self._successful_submits += 1


def format_live_order_confirmation(
    *,
    intent,
    order_request,
    order_row: dict,
    attempt_number: int,
    max_live_orders_total: int | None,
) -> str:
    lines = [
        "=== LIVE ORDER CONFIRMATION REQUIRED ===",
        f"attempt={attempt_number}",
        f"run_id={order_row.get('run_id') or 'live'}",
        f"intent_id={intent.intent_id}",
        f"market_id={intent.market_id}",
        f"token_id={intent.token_id}",
        f"side={order_request.side}",
        f"price={order_request.price}",
        f"size={order_request.size}",
        f"notional_usdc={intent.notional_usdc}",
        f"client_order_id={order_request.client_order_id}",
    ]
    if max_live_orders_total is not None:
        lines.append(f"max_live_orders_total={max_live_orders_total}")
    lines.append("Type 'y' to submit. Any other input aborts this order.")
    return "\n".join(lines)


def build_live_submit_guard(args: argparse.Namespace) -> LiveSubmitGuard | None:
    max_live_orders_total = getattr(args, "max_live_orders_total", None)
    require_confirmation = bool(getattr(args, "confirm_live_order", False))
    if max_live_orders_total is None and not require_confirmation:
        return None
    return OperatorConfirmLiveOrderGuard(
        max_live_orders_total=max_live_orders_total,
        require_confirmation=require_confirmation,
    )


def build_live_runtime(*, db_path: str | Path, run_id: str | None = None, config: ShortHorizonConfig | None = None, config_hash: str = "dev") -> StrategyRuntime:
    config = config or ShortHorizonConfig()
    run_context = RunContext(
        run_id=run_id or generate_run_id(),
        strategy_id=config.strategy_id,
        mode="live",
        config_hash=config_hash,
    )
    store = SQLiteRuntimeStore(db_path, run=run_context)
    strategy = ShortHorizon15mTouchStrategy(config=config)
    hydrate_open_orders = getattr(strategy, "hydrate_open_orders", None)
    if callable(hydrate_open_orders):
        hydrate_open_orders(store.load_non_terminal_orders())
    return StrategyRuntime(strategy=strategy, intent_store=store)


def run_stub_live(
    *,
    stub_event_log_path: str | Path,
    db_path: str | Path,
    run_id: str | None = None,
    config: ShortHorizonConfig | None = None,
    config_hash: str = "dev",
    execution_mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
) -> RunnerSummary:
    resolved_mode = ExecutionMode(str(execution_mode))
    if resolved_mode is ExecutionMode.LIVE:
        raise ValueError("stub mode does not support execution_mode=live")
    runtime = build_live_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    try:
        source = MarketDataSource.from_jsonl(stub_event_log_path)
        return drive_runtime_events(
            events=source.load(),
            runtime=runtime,
            logger_name="short_horizon.live_runner",
            completed_event_name="live_stub_run_completed",
            execution_mode=resolved_mode,
        )
    finally:
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


async def run_live(
    *,
    db_path: str | Path,
    run_id: str | None = None,
    config: ShortHorizonConfig | None = None,
    config_hash: str = "dev",
    source: LiveEventSource | None = None,
    max_events: int | None = None,
    max_runtime_seconds: float | None = None,
    execution_mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
    execution_client: PolymarketExecutionClient | None = None,
    live_submit_guard: LiveSubmitGuard | None = None,
) -> RunnerSummary:
    resolved_mode = ExecutionMode(str(execution_mode))
    runtime = build_live_runtime(db_path=db_path, run_id=run_id, config=config, config_hash=config_hash)
    client = execution_client
    if resolved_mode is ExecutionMode.LIVE:
        client = client or PolymarketExecutionClient()
        client.startup()
        reconcile_runtime_orders(runtime=runtime, execution_client=client, execution_mode=resolved_mode)
    source = source or build_live_source(execution_mode=resolved_mode, execution_client=client)
    try:
        await source.start()
        return await drive_runtime_event_stream(
            events=source.events,
            runtime=runtime,
            logger_name="short_horizon.live_runner",
            completed_event_name="live_run_completed",
            max_events=max_events,
            max_runtime_seconds=max_runtime_seconds,
            execution_mode=resolved_mode,
            execution_client=client,
            live_submit_guard=live_submit_guard,
        )
    finally:
        await source.stop()
        store = runtime.store
        close = getattr(store, "close", None)
        if callable(close):
            close()


def build_live_source(
    *,
    execution_mode: ExecutionMode | str = ExecutionMode.SYNTHETIC,
    execution_client: PolymarketExecutionClient | None = None,
) -> LiveEventSource:
    resolved_mode = ExecutionMode(str(execution_mode))
    if resolved_mode is not ExecutionMode.LIVE:
        return LiveEventSource()
    if execution_client is None:
        raise ValueError("execution_client is required for live execution mode")
    credentials = getattr(execution_client, "api_credentials", None)
    if not callable(credentials):
        raise TypeError("live execution mode requires execution_client.api_credentials() for authenticated user stream")
    return LiveEventSource(user_stream=PolymarketUserStream(auth=credentials()))


def reconcile_runtime_orders(
    *,
    runtime: StrategyRuntime,
    execution_client: PolymarketExecutionClient,
    execution_mode: ExecutionMode | str,
) -> int:
    resolved_mode = ExecutionMode(str(execution_mode))
    reconciled = ExecutionEngine(store=runtime.store, client=execution_client, mode=resolved_mode).reconcile_persisted_orders()
    hydrate_open_orders = getattr(runtime.strategy, "hydrate_open_orders", None)
    if callable(hydrate_open_orders):
        hydrate_open_orders(runtime.store.load_non_terminal_orders())
    return reconciled


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the short-horizon live shell in stub or real live mode")
    parser.add_argument("db_path", help="SQLite DB path for live-shell outputs")
    parser.add_argument("--mode", choices=("stub", "live"), default="stub", help="Input mode for the live runner")
    parser.add_argument(
        "--execution-mode",
        choices=tuple(mode.value for mode in ExecutionMode),
        default=ExecutionMode.SYNTHETIC.value,
        help="Execution mode: synthetic preserves Phase 2 behavior, dry_run translates orders without sending, live sends real orders",
    )
    parser.add_argument(
        "--allow-live-execution",
        action="store_true",
        help="Explicit operator acknowledgement required before --execution-mode live can touch mainnet",
    )
    parser.add_argument(
        "--confirm-live-order",
        action="store_true",
        help="Print each translated live order and require an interactive 'y' before submission",
    )
    parser.add_argument(
        "--max-live-orders-total",
        type=int,
        default=None,
        help="Optional hard cap on total approved live order submission attempts for this run",
    )
    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Operator-requested global safe mode: consume live data and reconciliation, but block new entry intents",
    )
    parser.add_argument(
        "--kill-switch",
        action="store_true",
        help="Cancel all active live orders across the account and exit immediately",
    )
    parser.add_argument(
        "--approve-allowances",
        action="store_true",
        help="Send Polygon approval transactions for Polymarket USDC + conditional-token spend before the live run",
    )
    parser.add_argument(
        "--bridge-polygon-usdc-to-usdce",
        action="store_true",
        help="Use the Polymarket bridge deposit flow to convert Polygon native USDC into Polygon USDC.e before the live run",
    )
    parser.add_argument(
        "--bridge-polygon-usdc-amount",
        default=None,
        help="Optional Polygon native USDC amount to bridge, in decimal token units; defaults to the wallet's full native USDC balance",
    )
    parser.add_argument("--stub-event-log-path", default=None, help="Path to a JSONL file of normalized stub events for --mode stub")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id; defaults to a fresh live_<suffix>")
    parser.add_argument("--config-hash", default="dev", help="Config hash label stored in runs table")
    parser.add_argument("--max-events", type=int, default=None, help="Optional cap on processed live events, useful for smoke tests")
    parser.add_argument("--max-runtime-seconds", type=float, default=None, help="Optional wall-clock cap for live probes")
    parser.add_argument("--collector-csv", default=None, help="Optional collector CSV path for post-run cross-validation")
    parser.add_argument(
        "--min-book-updates-per-minute",
        type=float,
        default=1.0,
        help="Fail the live probe if observed BookUpdate throughput falls below this rate over the probe window; set 0 to disable",
    )
    return parser


def generate_run_id() -> str:
    return f"live_{uuid.uuid4().hex[:12]}"


def _parse_usdc_amount_to_base_units(parser: argparse.ArgumentParser, flag_name: str, raw_value: str) -> int:
    try:
        amount = Decimal(str(raw_value).strip())
    except (InvalidOperation, ValueError):
        parser.error(f"{flag_name} must be a positive decimal amount")
    if amount <= 0:
        parser.error(f"{flag_name} must be positive")
    scaled = (amount * Decimal("1000000")).quantize(Decimal("1"), rounding=ROUND_DOWN)
    if scaled <= 0:
        parser.error(f"{flag_name} is too small after 6-decimal USDC rounding")
    normalized = scaled / Decimal("1000000")
    if normalized != amount:
        parser.error(f"{flag_name} must use at most 6 decimal places")
    return int(scaled)


def validate_cli_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> ExecutionMode:
    execution_mode = ExecutionMode(str(args.execution_mode))
    kill_switch = getattr(args, "kill_switch", False)
    if kill_switch:
        if getattr(args, "approve_allowances", False):
            parser.error("--approve-allowances cannot be combined with --kill-switch")
        if getattr(args, "bridge_polygon_usdc_to_usdce", False):
            parser.error("--bridge-polygon-usdc-to-usdce cannot be combined with --kill-switch")
        if args.mode != "live" or execution_mode is not ExecutionMode.LIVE:
            parser.error("--kill-switch requires --mode live and --execution-mode live")
        if not args.allow_live_execution:
            parser.error("--kill-switch requires --allow-live-execution")
        if not os.getenv(PRIVATE_KEY_ENV_VAR):
            parser.error(f"{PRIVATE_KEY_ENV_VAR} is required when --execution-mode live")
        return execution_mode

    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events must be positive when provided")
    if args.max_runtime_seconds is not None and args.max_runtime_seconds <= 0:
        parser.error("--max-runtime-seconds must be positive when provided")
    if args.max_live_orders_total is not None and args.max_live_orders_total <= 0:
        parser.error("--max-live-orders-total must be positive when provided")
    if args.mode == "stub" and execution_mode is ExecutionMode.LIVE:
        parser.error("--execution-mode live requires --mode live")
    if args.mode == "live" and execution_mode is ExecutionMode.LIVE and not args.allow_live_execution:
        parser.error("--execution-mode live requires --allow-live-execution")
    if args.mode == "live" and execution_mode is ExecutionMode.LIVE and not os.getenv(PRIVATE_KEY_ENV_VAR):
        parser.error(f"{PRIVATE_KEY_ENV_VAR} is required when --execution-mode live")
    if args.mode == "live" and execution_mode is ExecutionMode.LIVE:
        if args.max_events is None and args.max_runtime_seconds is None:
            parser.error("--execution-mode live requires --max-events or --max-runtime-seconds")
    if getattr(args, "confirm_live_order", False) or args.max_live_orders_total is not None:
        if args.mode != "live" or execution_mode is not ExecutionMode.LIVE:
            parser.error("--confirm-live-order and --max-live-orders-total require --mode live and --execution-mode live")
    if getattr(args, "approve_allowances", False):
        if args.mode != "live" or execution_mode is not ExecutionMode.LIVE:
            parser.error("--approve-allowances requires --mode live and --execution-mode live")
    bridge_requested = bool(getattr(args, "bridge_polygon_usdc_to_usdce", False))
    bridge_amount = getattr(args, "bridge_polygon_usdc_amount", None)
    if bridge_requested:
        if args.mode != "live" or execution_mode is not ExecutionMode.LIVE:
            parser.error("--bridge-polygon-usdc-to-usdce requires --mode live and --execution-mode live")
        if bridge_amount is not None:
            _parse_usdc_amount_to_base_units(parser, "--bridge-polygon-usdc-amount", bridge_amount)
    elif bridge_amount is not None:
        parser.error("--bridge-polygon-usdc-amount requires --bridge-polygon-usdc-to-usdce")
    if getattr(args, "confirm_live_order", False) and not sys.stdin.isatty():
        parser.error("--confirm-live-order requires an interactive TTY")
    return execution_mode


def apply_cli_config_overrides(config: ShortHorizonConfig | None, args: argparse.Namespace) -> ShortHorizonConfig | None:
    base = config or ShortHorizonConfig()
    if not getattr(args, "safe_mode", False):
        return config
    return replace(base, risk=replace(base.risk, global_safe_mode=True))


def execute_kill_switch(
    run_id: str | None = None,
    *,
    execution_client: PolymarketExecutionClient | None = None,
) -> KillSwitchSummary:
    logger = get_logger("short_horizon.live_runner", run_id=run_id or "kill_switch")
    logger.warning("kill_switch_engaged", action="canceling_all_orders")
    client = execution_client or PolymarketExecutionClient()
    try:
        client.startup()
    except Exception as exc:
        logger.error("kill_switch_startup_failed", error=str(exc))
        raise

    try:
        orders = client.list_open_orders()
    except Exception as exc:
        logger.error("kill_switch_list_failed", error=str(exc))
        raise

    open_order_count = len(orders)
    if not orders:
        summary = KillSwitchSummary(open_order_count=0, canceled_count=0, failed_count=0)
        logger.info(
            "kill_switch_completed",
            open_order_count=summary.open_order_count,
            canceled_count=summary.canceled_count,
            failed_count=summary.failed_count,
            message="No open orders found",
        )
        return summary

    canceled_count = 0
    failed_count = 0
    for order in orders:
        venue_order_id = str(order.order_id).strip()
        if not venue_order_id:
            failed_count += 1
            logger.error(
                "kill_switch_cancel_failed",
                venue_order_id=None,
                market_id=order.market_id,
                error="missing venue order id",
            )
            continue
        try:
            result = client.cancel_order(venue_order_id)
            if getattr(result, "success", True):
                canceled_count += 1
                logger.info(
                    "kill_switch_order_canceled",
                    venue_order_id=venue_order_id,
                    market_id=order.market_id,
                    venue_status=getattr(result, "status", None),
                )
            else:
                failed_count += 1
                logger.error(
                    "kill_switch_cancel_failed",
                    venue_order_id=venue_order_id,
                    market_id=order.market_id,
                    venue_status=getattr(result, "status", None),
                    error="venue_cancel_unsuccessful",
                )
        except Exception as exc:
            failed_count += 1
            logger.error(
                "kill_switch_cancel_failed",
                venue_order_id=venue_order_id,
                market_id=order.market_id,
                error=str(exc),
            )

    summary = KillSwitchSummary(
        open_order_count=open_order_count,
        canceled_count=canceled_count,
        failed_count=failed_count,
    )
    logger.warning(
        "kill_switch_completed",
        open_order_count=summary.open_order_count,
        canceled_count=summary.canceled_count,
        failed_count=summary.failed_count,
    )
    return summary


def execute_allowance_approve(
    run_id: str | None = None,
    *,
    execution_client: PolymarketExecutionClient | None = None,
) -> AllowanceApprovalSummary:
    logger = get_logger("short_horizon.live_runner", run_id=run_id or "allowance_approve")
    client = execution_client or PolymarketExecutionClient()
    if not _client_is_started(client):
        client.startup()
    results = client.approve_allowances()
    approved_count = 0
    already_approved_count = 0
    for result in results:
        if result.status == "approved":
            approved_count += 1
        else:
            already_approved_count += 1
        logger.info(
            "live_allowance_status",
            asset_type=result.asset_type,
            spender=result.spender,
            status=result.status,
            tx_hash=result.tx_hash,
        )
    summary = AllowanceApprovalSummary(
        total_actions=len(results),
        approved_count=approved_count,
        already_approved_count=already_approved_count,
    )
    logger.info(
        "live_allowance_completed",
        total_actions=summary.total_actions,
        approved_count=summary.approved_count,
        already_approved_count=summary.already_approved_count,
    )
    return summary


def execute_polygon_usdc_bridge(
    run_id: str | None = None,
    *,
    execution_client: PolymarketExecutionClient | None = None,
    amount_base_unit: int | None = None,
) -> PolygonUsdcBridgeSummary:
    logger = get_logger("short_horizon.live_runner", run_id=run_id or "bridge_polygon_usdc")
    client = execution_client or PolymarketExecutionClient()
    result: PolygonUsdcBridgeResult = client.bridge_polygon_usdc_to_usdce(amount_base_unit=amount_base_unit)
    collateral_delta = result.final_target_balance_base_unit - result.initial_target_balance_base_unit
    summary = PolygonUsdcBridgeSummary(
        source_amount_base_unit=result.source_amount_base_unit,
        deposit_address=result.deposit_address,
        wallet_transfer_tx_hash=result.wallet_transfer_tx_hash,
        bridge_status=result.bridge_status,
        bridge_tx_hash=result.bridge_tx_hash,
        quoted_target_amount_base_unit=result.quoted_target_amount_base_unit,
        collateral_delta_base_unit=collateral_delta,
    )
    logger.info(
        "live_bridge_completed",
        source_amount_base_unit=summary.source_amount_base_unit,
        deposit_address=summary.deposit_address,
        wallet_transfer_tx_hash=summary.wallet_transfer_tx_hash,
        bridge_status=summary.bridge_status,
        bridge_tx_hash=summary.bridge_tx_hash,
        quoted_target_amount_base_unit=summary.quoted_target_amount_base_unit,
        collateral_delta_base_unit=summary.collateral_delta_base_unit,
    )
    return summary


def _client_is_started(client: object) -> bool:
    if getattr(client, "started", False):
        return True
    return getattr(client, "_client", None) is not None


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    execution_mode = validate_cli_args(parser, args)
    config = apply_cli_config_overrides(None, args)
    configure_logging()

    if getattr(args, "kill_switch", False):
        try:
            summary = execute_kill_switch(args.run_id)
        except Exception:
            raise SystemExit(1) from None
        if summary.failed_count:
            raise SystemExit(1)
        return

    logger = get_logger("short_horizon.live_runner", run_id=args.run_id)
    logger.info(
        "live_runner_starting",
        input_mode=args.mode,
        execution_mode=execution_mode.value,
        safe_mode=bool(args.safe_mode),
        approve_allowances=bool(getattr(args, "approve_allowances", False)),
        bridge_polygon_usdc_to_usdce=bool(getattr(args, "bridge_polygon_usdc_to_usdce", False)),
        bridge_polygon_usdc_amount=getattr(args, "bridge_polygon_usdc_amount", None),
        confirm_live_order=bool(getattr(args, "confirm_live_order", False)),
        max_live_orders_total=getattr(args, "max_live_orders_total", None),
        db_path=str(args.db_path),
        max_events=args.max_events,
        max_runtime_seconds=args.max_runtime_seconds,
    )
    if args.mode == "stub":
        if not args.stub_event_log_path:
            parser.error("--stub-event-log-path is required when --mode stub")
        summary = run_stub_live(
            stub_event_log_path=args.stub_event_log_path,
            db_path=args.db_path,
            run_id=args.run_id,
            config=config,
            config_hash=args.config_hash,
            execution_mode=execution_mode,
        )
    else:
        execution_client = None
        if execution_mode is ExecutionMode.LIVE and (
            getattr(args, "approve_allowances", False) or getattr(args, "bridge_polygon_usdc_to_usdce", False)
        ):
            execution_client = PolymarketExecutionClient()
        if execution_mode is ExecutionMode.LIVE and getattr(args, "bridge_polygon_usdc_to_usdce", False):
            amount_base_unit = None
            if getattr(args, "bridge_polygon_usdc_amount", None) is not None:
                amount_base_unit = _parse_usdc_amount_to_base_units(
                    parser,
                    "--bridge-polygon-usdc-amount",
                    str(args.bridge_polygon_usdc_amount),
                )
            execute_polygon_usdc_bridge(
                args.run_id,
                execution_client=execution_client,
                amount_base_unit=amount_base_unit,
            )
        if execution_mode is ExecutionMode.LIVE and getattr(args, "approve_allowances", False):
            execute_allowance_approve(args.run_id, execution_client=execution_client)
        summary = asyncio.run(
            run_live(
                db_path=args.db_path,
                run_id=args.run_id,
                config=config,
                config_hash=args.config_hash,
                max_events=args.max_events,
                max_runtime_seconds=args.max_runtime_seconds,
                execution_mode=execution_mode,
                execution_client=execution_client,
                live_submit_guard=build_live_submit_guard(args),
            )
        )
    logger = get_logger("short_horizon.live_runner", run_id=summary.run_id)
    probe_summary = summarize_probe_db(args.db_path, run_id=summary.run_id)
    logger.info(
        "live_runner_completed",
        run_id=summary.run_id,
        input_events=summary.event_count,
        order_intents=summary.order_intents,
        synthetic_order_events=summary.synthetic_order_events,
        execution_mode=execution_mode.value,
        safe_mode=bool(args.safe_mode),
        db_path=str(summary.db_path),
    )
    logger.info(
        "live_probe_summary",
        run_id=probe_summary.run_id,
        total_events=probe_summary.total_events,
        market_state_updates=probe_summary.market_state_updates,
        book_updates=probe_summary.book_updates,
        trade_ticks=probe_summary.trade_ticks,
        order_events=probe_summary.order_events,
        distinct_markets=probe_summary.distinct_markets,
        distinct_tokens=probe_summary.distinct_tokens,
        first_event_time=probe_summary.first_event_time,
        last_event_time=probe_summary.last_event_time,
        window_minutes=probe_summary.window_minutes,
        book_updates_per_minute=probe_summary.book_updates_per_minute,
    )
    if args.min_book_updates_per_minute and args.min_book_updates_per_minute > 0:
        assert_min_book_updates_per_minute(
            probe_summary,
            min_rate=args.min_book_updates_per_minute,
        )
    if args.collector_csv:
        validation = maybe_cross_validate_probe_against_collector(
            args.db_path,
            run_id=summary.run_id,
            collector_csv_path=args.collector_csv,
        )
        if validation is None:
            logger.warning(
                "live_probe_cross_validation_skipped",
                run_id=summary.run_id,
                collector_csv_path=str(args.collector_csv),
                reason="collector_csv_missing",
            )
        else:
            logger.info(
                "live_probe_cross_validation",
                run_id=validation.run_id,
                collector_rows=validation.collector_rows,
                probe_markets=validation.probe_markets,
                collector_markets=validation.collector_markets,
                overlapping_markets=validation.overlapping_markets,
                probe_tokens=validation.probe_tokens,
                collector_tokens=validation.collector_tokens,
                overlapping_tokens=validation.overlapping_tokens,
            )


if __name__ == "__main__":
    main()
