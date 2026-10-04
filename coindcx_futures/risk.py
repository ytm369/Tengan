from __future__ import annotations

import math
from dataclasses import dataclass

from .config import RiskLimits
from .types import InstrumentSpec, RiskDecision, TradeProposal


@dataclass(frozen=True, slots=True)
class AccountRiskState:
    open_positions: int = 0
    open_pairs: frozenset[str] = frozenset()
    total_exposure_inr: float = 0.0
    realized_pnl_today_inr: float = 0.0
    usdt_inr_conversion: float = 0.0


class RiskEngine:
    def assess(
        self,
        proposal: TradeProposal,
        spec: InstrumentSpec,
        limits: RiskLimits,
        account: AccountRiskState,
        *,
        live_armed: bool,
        model_confidence: int,
        minimum_model_confidence: int,
    ) -> RiskDecision:
        if not live_armed:
            return RiskDecision(False, reason="live execution is not armed")
        if not limits.configured():
            return RiskDecision(False, reason="all hard risk limits must be configured")
        if model_confidence < minimum_model_confidence:
            return RiskDecision(False, reason="model confidence is below the configured minimum")
        if proposal.pair in account.open_pairs:
            return RiskDecision(False, reason="a position already exists for this pair")
        if account.open_positions >= limits.max_open_positions:
            return RiskDecision(False, reason="maximum concurrent positions reached")
        if account.realized_pnl_today_inr <= -limits.max_daily_loss_inr:
            return RiskDecision(False, reason="daily loss limit reached")
        if proposal.side not in {"long", "short"}:
            return RiskDecision(False, reason="invalid trade side")
        if not all(_positive(value) for value in (proposal.entry_price, proposal.stop_loss, proposal.take_profit)):
            return RiskDecision(False, reason="entry, stop and target must be positive finite prices")
        if proposal.side == "long" and not proposal.stop_loss < proposal.entry_price < proposal.take_profit:
            return RiskDecision(False, reason="long stop/target must bracket the entry")
        if proposal.side == "short" and not proposal.take_profit < proposal.entry_price < proposal.stop_loss:
            return RiskDecision(False, reason="short stop/target must bracket the entry")
        conversion = account.usdt_inr_conversion
        if not _positive(conversion) or not _positive(spec.unit_contract_value):
            return RiskDecision(False, reason="INR conversion or contract value is unavailable")
        if not _positive(spec.quantity_increment) or not _positive(spec.min_quantity) or not _positive(spec.max_quantity):
            return RiskDecision(False, reason="exchange quantity increments or bounds are unavailable")
        stop_risk_per_unit_inr = abs(proposal.entry_price - proposal.stop_loss) * spec.unit_contract_value * conversion
        exposure_per_unit_inr = proposal.entry_price * spec.unit_contract_value * conversion
        if not _positive(stop_risk_per_unit_inr) or not _positive(exposure_per_unit_inr):
            return RiskDecision(False, reason="could not calculate INR risk or exposure")
        exposure_headroom = limits.max_total_exposure_inr - account.total_exposure_inr
        if exposure_headroom <= 0:
            return RiskDecision(False, reason="maximum total exposure reached")
        by_risk = limits.max_risk_per_trade_inr / stop_risk_per_unit_inr
        by_exposure = exposure_headroom / exposure_per_unit_inr
        allowed_quantity = min(by_risk, by_exposure, spec.max_quantity or math.inf)
        if proposal.quantity > 0:
            allowed_quantity = min(allowed_quantity, proposal.quantity)
        quantity = _round_down(allowed_quantity, spec.quantity_increment)
        if quantity < max(spec.min_quantity, 0):
            return RiskDecision(False, reason="safe quantity is below the exchange minimum")
        if quantity <= 0:
            return RiskDecision(False, reason="risk limits allow no positive order quantity")
        requested_leverage = max(1, int(proposal.leverage))
        leverage = min(requested_leverage, limits.max_leverage, int(spec.max_leverage or 1))
        if leverage < 1:
            return RiskDecision(False, reason="no valid leverage is available")
        return RiskDecision(True, quantity=quantity, leverage=leverage,
                            reason=f"approved at {leverage}x leverage within configured hard caps")


def _positive(value: float) -> bool:
    return math.isfinite(value) and value > 0


def _round_down(value: float, increment: float) -> float:
    if increment <= 0:
        return max(0.0, value)
    return math.floor(value / increment) * increment
