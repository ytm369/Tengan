from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .storage import AuditStore
from .types import MarketEvent


@dataclass(frozen=True, slots=True)
class PaperPosition:
    pair: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit: float
    opened_at_ms: int
    confidence: int


class PaperOutcomeTracker:
    """Tracks simulated entries against live public last-price updates."""

    def __init__(self, store: AuditStore) -> None:
        self.store = store
        self.open_positions: dict[str, PaperPosition] = {
            row["pair"]: PaperPosition(
                pair=row["pair"], side=row["side"], entry_price=float(row["current_price"]),
                stop_loss=float(row["stop_loss"]), take_profit=float(row["take_profit"]),
                opened_at_ms=int(row.get("timestamp_ms", 0)), confidence=int(row.get("confidence", 0)),
            )
            for row in store.load_open_paper_trades()
            if _valid_bracket(row.get("side"), row.get("current_price"), row.get("stop_loss"), row.get("take_profit"))
        }

    def open(self, *, pair: str, side: str, entry_price: float, stop_loss: float,
             take_profit: float, timestamp_ms: int, confidence: int,
             score: float, rationale: str) -> bool:
        if pair in self.open_positions or not _valid_bracket(side, entry_price, stop_loss, take_profit):
            return False
        position = PaperPosition(pair, side, entry_price, stop_loss, take_profit, timestamp_ms, confidence)
        self.open_positions[pair] = position
        self.store.log_trade(timestamp_ms, "paper_signal", {
            "pair": pair, "side": side, "current_price": entry_price,
            "stop_loss": stop_loss, "take_profit": take_profit,
            "timestamp_ms": timestamp_ms, "confidence": confidence,
            "scanner_score": score, "rationale": rationale,
            "note": "paper signal only; no exchange order was sent",
        })
        return True

    def on_market_event(self, event: MarketEvent, latest_price: float | None) -> None:
        if event.kind != "ticker" or not any(key in event.payload for key in ("ls", "price", "p")):
            return
        position = self.open_positions.get(event.pair)
        if position is None or event.timestamp_ms < position.opened_at_ms:
            return
        if latest_price is None or not math.isfinite(latest_price) or latest_price <= 0:
            return
        reason = _exit_reason(position, latest_price)
        if reason is None:
            return
        signed_return = (latest_price - position.entry_price) / position.entry_price
        if position.side == "short":
            signed_return *= -1
        payload = {
            "pair": position.pair, "side": position.side,
            "entry_price": position.entry_price, "exit_price": latest_price,
            "opened_at_ms": position.opened_at_ms, "closed_at_ms": event.timestamp_ms,
            "duration_ms": max(0, event.timestamp_ms - position.opened_at_ms),
            "return_pct": signed_return * 100, "exit_reason": reason,
            "confidence": position.confidence,
        }
        self.store.log_paper_outcome(payload)
        del self.open_positions[event.pair]


def _valid_bracket(side: Any, entry: Any, stop: Any, target: Any) -> bool:
    try:
        entry, stop, target = float(entry), float(stop), float(target)
    except (TypeError, ValueError):
        return False
    if not all(math.isfinite(value) and value > 0 for value in (entry, stop, target)):
        return False
    if side == "long":
        return stop < entry < target
    if side == "short":
        return target < entry < stop
    return False


def _exit_reason(position: PaperPosition, price: float) -> str | None:
    if position.side == "long":
        if price <= position.stop_loss:
            return "stop_loss"
        if price >= position.take_profit:
            return "take_profit"
    else:
        if price >= position.stop_loss:
            return "stop_loss"
        if price <= position.take_profit:
            return "take_profit"
    return None
