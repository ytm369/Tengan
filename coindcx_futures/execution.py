from __future__ import annotations

import asyncio
import datetime as dt
import logging
import math
import time
from dataclasses import replace

from .coindcx import CoinDCXPrivateClient, CoinDCXPublicClient
from .config import Settings
from .risk import AccountRiskState, RiskEngine
from .state import MarketState
from .storage import AuditStore
from .types import Candidate, CandidateReview, InstrumentSpec, TradeProposal

LOG = logging.getLogger(__name__)


class LiveExecutor:
    """Fail-closed execution path. Construct only after explicit live arming."""

    def __init__(self, private: CoinDCXPrivateClient, public: CoinDCXPublicClient,
                 state: MarketState, store: AuditStore, settings: Settings) -> None:
        self.private = private
        self.public = public
        self.state = state
        self.store = store
        self.settings = settings
        self.risk = RiskEngine()
        self.halted = False

    async def execute(self, candidate: Candidate, review: CandidateReview) -> bool:
        now_ms = int(time.time() * 1000)
        if self.halted:
            return False
        snapshot = await self.state.snapshot(candidate.pair)
        if snapshot is None or snapshot.price is None:
            return self._deny("latest price unavailable", candidate, review)
        if now_ms - snapshot.field_timestamps.get("ticker", 0) > self.settings.max_entry_age_seconds * 1000:
            return self._deny("market data is stale", candidate, review)
        if review.decision != candidate.side or review.confidence < self.settings.minimum_model_confidence:
            return self._deny("model review did not pass the entry gate", candidate, review)
        entry = float(snapshot.price)
        stop = review.stop_loss
        target = review.take_profit
        if stop is None or target is None or not _brackets(candidate.side, entry, stop, target):
            return self._deny("model protection levels do not bracket the live price", candidate, review)
        try:
            spec = await self.public.get_instrument(candidate.pair, "INR")
            account, _positions = await self._account_state()
        except Exception as exc:
            return self._deny(f"live account verification failed: {exc}", candidate, review)
        snapshot = await self.state.snapshot(candidate.pair)
        now_ms = int(time.time() * 1000)
        if snapshot is None or snapshot.price is None or now_ms - snapshot.field_timestamps.get("ticker", 0) > self.settings.max_entry_age_seconds * 1000:
            return self._deny("market data became stale during account verification", candidate, review)
        entry = float(snapshot.price)
        if not _brackets(candidate.side, entry, stop, target):
            return self._deny("market moved outside the proposed protection levels", candidate, review)
        leverage = self.settings.risk_limits.max_leverage
        proposal = TradeProposal(
            pair=candidate.pair,
            side=candidate.side,
            entry_price=entry,
            stop_loss=stop,
            take_profit=target,
            quantity=review.quantity_suggestion or 0.0,
            leverage=leverage,
        )
        decision = self.risk.assess(
            proposal, spec, self.settings.risk_limits, account,
            live_armed=True,
            model_confidence=review.confidence,
            minimum_model_confidence=self.settings.minimum_model_confidence,
        )
        self.store.log_trade(now_ms, "risk_decision", {"candidate": candidate, "review": review, "decision": decision})
        if not decision.approved:
            LOG.info("Risk gate blocked %s: %s", candidate.pair, decision.reason)
            return False
        proposal = replace(proposal, quantity=decision.quantity, leverage=decision.leverage)
        return await self._submit_and_protect(proposal, spec)

    async def _account_state(self) -> tuple[AccountRiskState, list[dict]]:
        positions = await self.private.get_positions("INR")
        conversion = await self.private.get_conversion_rate()
        transactions = await self.private.get_today_transactions("INR")
        today = dt.datetime.now().astimezone().date()
        pnl = 0.0
        for row in transactions:
            created = _epoch_ms(row.get("created_at"))
            if created is None:
                raise RuntimeError("CoinDCX returned a transaction without a usable timestamp")
            if dt.datetime.fromtimestamp(created / 1000, tz=dt.datetime.now().astimezone().tzinfo).date() == today:
                pnl += _strict_number(row.get("amount"), "transaction PnL")
        open_rows = []
        for row in positions:
            active_quantity = _strict_number(row.get("active_pos"), "position quantity")
            if abs(active_quantity) > 0:
                open_rows.append(row)
        exposure = 0.0
        for row in open_rows:
            pair = str(row.get("pair", ""))
            quantity = abs(_strict_number(row.get("active_pos"), "position quantity"))
            if not pair or quantity <= 0:
                raise RuntimeError("CoinDCX position data is incomplete; cannot calculate exposure")
            snapshot = await self.state.snapshot(pair)
            ticker_time = snapshot.field_timestamps.get("ticker", 0) if snapshot else 0
            ticker_age = int(time.time() * 1000) - ticker_time
            if (snapshot is None or snapshot.price is None or ticker_time <= 0
                    or ticker_age < -5_000
                    or ticker_age > self.settings.max_entry_age_seconds * 1000):
                raise RuntimeError(f"CoinDCX ticker data for open position {pair} is unavailable or stale")
            mark = _strict_number(snapshot.price, "open-position ticker price")
            if mark <= 0:
                raise RuntimeError("CoinDCX returned a non-positive open-position ticker price")
            spec = await self.public.get_instrument(pair, "INR")
            exposure += quantity * mark * spec.unit_contract_value * conversion
        state = AccountRiskState(
            open_positions=len(open_rows),
            open_pairs=frozenset(str(row.get("pair")) for row in open_rows),
            total_exposure_inr=exposure,
            realized_pnl_today_inr=pnl,
            usdt_inr_conversion=conversion,
        )
        return state, positions

    async def _submit_and_protect(self, proposal: TradeProposal, spec: InstrumentSpec) -> bool:
        submitted_at = int(time.time() * 1000)
        position_id = ""
        self.store.log_trade(submitted_at, "order_intent", proposal)
        try:
            response = await self.private.create_market_order(proposal, "INR")
            self.store.log_trade(int(time.time() * 1000), "order_response", response)
            order = _first_order(response)
            if order and str(order.get("status", "")).lower() == "rejected":
                self.store.log_trade(int(time.time() * 1000), "order_rejected", {
                    "pair": proposal.pair, "order": order,
                })
                LOG.info("CoinDCX rejected the market order for %s", proposal.pair)
                return False
            position = await self._wait_for_position(proposal.pair, proposal.side)
            if position is None:
                if order and await self._cancel_remaining_order(order, proposal.pair, proposal.side):
                    position = await self._wait_for_position(proposal.pair, proposal.side, attempts=2)
            if position is None:
                self.halted = True
                self.store.log_trade(int(time.time() * 1000), "execution_halt", {
                    "pair": proposal.pair, "reason": "order submitted but position could not be confirmed; reconcile manually",
                })
                return False
            position_id = str(position.get("id", ""))
            if not await self._cancel_remaining_order(order, proposal.pair, proposal.side):
                await self._emergency_exit(position_id, proposal.pair, "could not confirm cancellation of unfilled market-order remainder")
                return False
            confirmed_position = await self._get_position(proposal.pair, proposal.side)
            if confirmed_position is None:
                await self._emergency_exit(position_id, proposal.pair, "position disappeared before protective orders were attached")
                return False
            position = confirmed_position
            position_id = str(position.get("id", ""))
            fill_price = _float(position.get("avg_price")) or proposal.entry_price
            filled_quantity = abs(_float(position.get("active_pos")))
            if not position_id or not _brackets(proposal.side, fill_price, proposal.stop_loss, proposal.take_profit):
                await self._emergency_exit(position_id, proposal.pair, "filled price invalidated protection levels")
                return False
            if filled_quantity <= 0 or filled_quantity > proposal.quantity + max(1e-9, proposal.quantity * 1e-8):
                await self._emergency_exit(position_id, proposal.pair, "confirmed fill quantity is invalid or exceeds approved size")
                return False
            protection = await self.private.create_tpsl(position_id, proposal.stop_loss, proposal.take_profit)
            self.store.log_trade(int(time.time() * 1000), "protection_response", protection)
            if not _protection_succeeded(protection):
                await self._emergency_exit(position_id, proposal.pair, "CoinDCX rejected stop-loss or take-profit")
                return False
            self.store.log_trade(int(time.time() * 1000), "protected_position", {
                "pair": proposal.pair, "position_id": position_id, "fill_price": fill_price,
                "quantity": filled_quantity, "approved_quantity": proposal.quantity,
                "stop_loss": proposal.stop_loss,
                "take_profit": proposal.take_profit,
            })
            LOG.warning("LIVE position opened and protected: %s %s", proposal.side, proposal.pair)
            return True
        except Exception as exc:
            # A timeout may happen after the exchange accepted an order, so halt until reconciled.
            if position_id:
                await self._emergency_exit(position_id, proposal.pair, f"execution failed before protection was confirmed: {exc}")
            else:
                self.halted = True
                self.store.log_trade(int(time.time() * 1000), "execution_halt", {
                    "pair": proposal.pair, "reason": f"uncertain order/protection state: {exc}",
                })
                LOG.exception("Execution state uncertain for %s; new entries halted", proposal.pair)
            return False

    async def _wait_for_position(self, pair: str, side: str, attempts: int = 6) -> dict | None:
        for _ in range(attempts):
            position = await self._get_position(pair, side)
            if position is not None:
                return position
            await asyncio.sleep(0.5)
        return None

    async def _get_position(self, pair: str, side: str) -> dict | None:
        positions = await self.private.get_positions("INR")
        for row in positions:
            if row.get("pair") != pair:
                continue
            quantity = _float(row.get("active_pos"))
            if (side == "long" and quantity > 0) or (side == "short" and quantity < 0):
                return row
        return None

    async def _cancel_remaining_order(self, order: dict | None, pair: str, side: str) -> bool:
        if not order:
            return False
        order_id = str(order.get("id", ""))
        status = str(order.get("status", "")).lower()
        remaining = _float(order.get("remaining_quantity"))
        if not order_id:
            return status in {"filled", "rejected", "cancelled", "canceled", "partially_cancelled"} and remaining <= 0
        orders = await self.private.get_open_orders("buy" if side == "long" else "sell", "INR")
        pending = next((row for row in orders if str(row.get("id", "")) == order_id), None)
        if pending is None:
            return True
        result = await self.private.cancel_order(order_id)
        self.store.log_trade(int(time.time() * 1000), "order_remainder_cancel", {
            "pair": pair, "order_id": order_id, "response": result,
        })
        if not _cancel_succeeded(result):
            return False
        for _ in range(6):
            orders = await self.private.get_open_orders("buy" if side == "long" else "sell", "INR")
            if not any(str(row.get("id", "")) == order_id for row in orders):
                return True
            await asyncio.sleep(0.25)
        return False

    async def _emergency_exit(self, position_id: str, pair: str, reason: str) -> None:
        try:
            if position_id:
                response = await self.private.exit_position(position_id)
                self.store.log_trade(int(time.time() * 1000), "emergency_exit", {
                    "pair": pair, "reason": reason, "response": response,
                })
            else:
                self.store.log_trade(int(time.time() * 1000), "emergency_exit_unavailable", {
                    "pair": pair, "reason": reason,
                })
        except Exception as exc:
            self.store.log_trade(int(time.time() * 1000), "emergency_exit_failed", {
                "pair": pair, "reason": reason, "error": str(exc),
            })
        finally:
            self.halted = True
            self.store.log_trade(int(time.time() * 1000), "execution_halt", {
                "pair": pair, "reason": reason,
            })
            LOG.critical("Protection failed for %s. Emergency exit attempted; new entries halted.", pair)

    def _deny(self, reason: str, candidate: Candidate, review: CandidateReview) -> bool:
        self.store.log_trade(int(time.time() * 1000), "entry_rejected", {
            "pair": candidate.pair, "reason": reason, "review": review,
        })
        LOG.info("Entry rejected for %s: %s", candidate.pair, reason)
        return False


def _protection_succeeded(response: dict) -> bool:
    if not isinstance(response, dict):
        return False
    return _protection_leg_succeeded(response.get("take_profit")) and _protection_leg_succeeded(response.get("stop_loss"))


def _protection_leg_succeeded(value) -> bool:
    if not isinstance(value, dict) or value.get("success") is False or value.get("error"):
        return False
    # CoinDCX returns an order object (with id/status) for success, and an
    # explicit success:false/error object for a failed TP or SL leg.
    return value.get("success") is True or bool(value.get("id"))


def _first_order(response) -> dict | None:
    if isinstance(response, list):
        return next((item for item in response if isinstance(item, dict)), None)
    if isinstance(response, dict):
        data = response.get("data")
        if isinstance(data, list):
            return next((item for item in data if isinstance(item, dict)), None)
        return response
    return None


def _cancel_succeeded(response) -> bool:
    if not isinstance(response, dict):
        return False
    return response.get("code") in {200, "200"} or response.get("status") in {200, "success"} or response.get("message") == "success"


def _brackets(side: str, entry: float, stop: float, target: float) -> bool:
    values = (entry, stop, target)
    if not all(math.isfinite(value) and value > 0 for value in values):
        return False
    if side == "long":
        return stop < entry < target
    return side == "short" and target < entry < stop


def _float(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _strict_number(value, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"CoinDCX returned an invalid {label}") from exc
    if not math.isfinite(result):
        raise RuntimeError(f"CoinDCX returned a non-finite {label}")
    return result


def _epoch_ms(value) -> int | None:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 10_000_000_000 else parsed * 1000
