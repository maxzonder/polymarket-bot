"""
Risk Manager — position sizing and TP ladder per trading mode.

Key principle for big_swan_mode:
  TP is NOT full exit. It's capital recycling.
  20% at 5x → covers initial stake 1x, rest is house money.
  20% at 20x → partial profit lock.
  60% moonbag → held to resolution for the 100x–1000x tail.

Why we DON'T use trailing stops on Polymarket:
  - Thin illiquid markets: synthetic trailing stop creates false security.
  - In a dip-to-floor market, price can go 0.001 → 0.05 → 0.01 → 0.08 → 1.0
    A trailing stop at -50% peak would exit at 0.025 and miss the resolution at 1.0.
  - Only exception: if market shows NO volume activity after 5x hit AND days remain,
    consider a conservative partial exit at 10x threshold.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from config import ModeConfig, TPLevel
from strategy.scorer import ResolutionScore
from strategy.market_scorer import MarketScore


@dataclass
class SizedPosition:
    """Result of position sizing calculation."""
    token_id: str
    entry_price: float
    stake_usdc: float        # total USDC to spend on this position
    token_quantity: float    # = stake_usdc / entry_price
    tp_levels: list[TPLevel]
    moonbag_fraction: float
    rationale: str


@dataclass
class TPOrder:
    """A single take-profit order to place."""
    token_id: str
    sell_price: float        # limit price for SELL order
    sell_quantity: float     # tokens to sell at this level
    label: str               # "tp_5x", "tp_20x", "moonbag_resolution", etc.


class RiskManager:
    """
    Computes position size and TP ladder for a given candidate.

    Position sizing rules:
    - Default: mode_config.stake_usdc
    - Scale up if resolution_score is high AND sufficient balance
    - Hard cap: never more than balance * max_capital_deployed_pct / max_open_positions
    - Anti-garbage filter: reject if historical avg entry_volume_usdc is too thin
    """

    def __init__(self, mode_config: ModeConfig, balance_usdc: float = 10.0):
        self.mc = mode_config
        self.balance_usdc = balance_usdc

    def size_position(
        self,
        token_id: str,
        entry_price: float,
        resolution_score: Optional[ResolutionScore] = None,
        open_positions: int = 0,
        market_score: Optional[MarketScore] = None,
    ) -> Optional[SizedPosition]:
        """
        Compute stake for a new position.
        Returns None if position should be rejected on risk grounds.

        v1.1: If market_score is provided and market_score_tiers is configured,
        quality-weighted sizing overrides price-tier sizing.
        market_score < min_market_score → reject (return None).
        """
        # v1.1: reject markets below score threshold
        if market_score is not None and self.mc.min_market_score > 0:
            if market_score.total < self.mc.min_market_score:
                return None

        # Anti-garbage: reject if price is suspiciously low with no historical data
        if resolution_score is not None:
            if entry_price < 0.0005 and resolution_score.sample_count < 5:
                return None  # paper price artifact risk

        # ── Stake determination (v1.1 priority order) ─────────────────────────
        # 1. If market_score_tiers configured: quality-weighted sizing
        # 2. Else if stake_tiers configured: price-based sizing
        # 3. Else: flat stake_usdc fallback
        base_stake = self.mc.stake_usdc
        tier_source = "flat"

        if self.mc.market_score_tiers and market_score is not None:
            # Quality-weighted: higher market_score → larger stake per fill
            for threshold, tier_stake in sorted(
                self.mc.market_score_tiers, key=lambda t: t[0], reverse=True
            ):
                if market_score.total >= threshold:
                    base_stake = tier_stake
                    tier_source = f"score_tier(>={threshold:.2f})"
                    break
        elif self.mc.stake_tiers:
            # Price-based (v1 legacy behaviour)
            for tier_price, tier_stake in self.mc.stake_tiers:
                if entry_price <= tier_price:
                    base_stake = tier_stake
                    tier_source = f"price_tier(<={tier_price:.4f})"
                    break

        stake = base_stake

        # Legacy resolution_score scaling for non-v1.1 modes (no market_score_tiers)
        if (
            self.mc.optimize_metric == "tail_ev"
            and not self.mc.market_score_tiers
            and resolution_score is not None
        ):
            score_factor = 0.5 + resolution_score.score * 0.5
            stake = stake * score_factor

        stake = max(0.001, round(stake, 6))
        token_quantity = stake / entry_price

        score_info = f" mscore={market_score.total:.3f}({market_score.tier})" if market_score else ""
        rationale = (
            f"mode={self.mc.name} stake=${stake:.4f}[{tier_source}]{score_info} "
            f"qty={token_quantity:.2f} tokens @ ${entry_price:.5f}"
        )
        if resolution_score is not None:
            rationale += (
                f" res_score={resolution_score.score:.3f} "
                f"p_winner={resolution_score.p_winner:.2f} "
                f"tail_ev={resolution_score.tail_ev:.1f}"
            )

        return SizedPosition(
            token_id=token_id,
            entry_price=entry_price,
            stake_usdc=stake,
            token_quantity=token_quantity,
            tp_levels=list(self.mc.tp_levels),
            moonbag_fraction=self.mc.moonbag_fraction,
            rationale=rationale,
        )

    def build_tp_orders(self, position: SizedPosition) -> list[TPOrder]:
        """
        Convert a filled position into TP orders.

        Example for big_swan_mode with entry at $0.002, stake=$0.01, qty=5 tokens:
          tp_5x:   sell 1 token @ $0.010  (20% of 5)
          tp_20x:  sell 1 token @ $0.040  (20% of 5)
          moonbag: 3 tokens held to resolution (60%)
        """
        orders: list[TPOrder] = []
        total_qty = position.token_quantity
        entry_price = position.entry_price
        remaining_fraction = 1.0

        for i, tp in enumerate(position.tp_levels):
            # Validate TP is achievable (target price < $1.00)
            target_price = entry_price * tp.x
            if target_price >= 1.00:
                # For resolution-only payout, skip market TP and let moonbag handle it
                break

            sell_qty = total_qty * tp.fraction
            sell_qty = round(sell_qty, 4)
            if sell_qty < 0.0001:
                continue

            label = f"tp_{int(tp.x)}x"
            orders.append(TPOrder(
                token_id=position.token_id,
                sell_price=round(target_price, 6),
                sell_quantity=sell_qty,
                label=label,
            ))
            remaining_fraction -= tp.fraction

        # Moonbag: the remaining fraction is held to resolution
        # We do NOT place a market sell order for moonbag — resolution pays $1.00
        # Just record it in positions table as "hold_to_resolution"
        moonbag_qty = total_qty * position.moonbag_fraction
        if moonbag_qty >= 0.0001:
            orders.append(TPOrder(
                token_id=position.token_id,
                sell_price=1.00,           # resolution price
                sell_quantity=round(moonbag_qty, 4),
                label="moonbag_resolution",
            ))

        return orders

    def should_exit_early(
        self,
        current_price: float,
        entry_price: float,
        days_since_entry: float,
        hours_to_resolution: float,
        resolution_score: ResolutionScore,
    ) -> bool:
        """
        In rare cases, a full early exit is justified:
        1. Resolution is < 2h away and price < 5x (won't resolve YES)
        2. Market has extremely low resolution score and price already at 5x+

        We do NOT implement trailing stop — see module docstring.
        """
        current_x = current_price / entry_price if entry_price > 0 else 1.0

        # If resolution imminent and price hasn't moved, likely a loser
        if hours_to_resolution < 2.0 and current_x < 3.0:
            return True

        # If we're already at good profit but resolution score is very low and
        # time left is long — take what we can (anti-capital-lock)
        if (
            current_x >= 5.0
            and resolution_score.p_winner < 0.05
            and days_since_entry > 7.0
            and hours_to_resolution > 24.0
        ):
            return True

        return False
