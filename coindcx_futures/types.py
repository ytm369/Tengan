from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class MarketEvent:
    pair: str
    kind: str
    timestamp_ms: int
    source: str
    payload: dict[str, Any]
    timeframe: str | None = None
    source_sequence: int | None = None


@dataclass(frozen=True, slots=True)
class Candle:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    pair: str
    margin_currency: str = "INR"
    quote_currency: str = "USDT"
    price_increment: float = 0.0
    quantity_increment: float = 0.0
    min_quantity: float = 0.0
    max_quantity: float = 0.0
    min_notional: float = 0.0
    unit_contract_value: float = 1.0
    max_leverage: float = 1.0


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    pair: str
    revision: int
    updated_at_ms: int
    field_timestamps: dict[str, int] = field(default_factory=dict)
    price: float | None = None
    mark_price: float | None = None
    volume_24h: float | None = None
    change_24h_pct: float | None = None
    funding_rate: float | None = None
    bids: dict[str, str] = field(default_factory=dict)
    asks: dict[str, str] = field(default_factory=dict)
    candles: dict[str, tuple[Candle, ...]] = field(default_factory=dict)
    long_percent: float | None = None
    short_percent: float | None = None
    open_interest: float | None = None


@dataclass(frozen=True, slots=True)
class NodeResult:
    node: str
    pair: str
    input_revision: int
    timestamp_ms: int
    score: float | None
    features: dict[str, Any] = field(default_factory=dict)
    available: bool = True


@dataclass(frozen=True, slots=True)
class Candidate:
    pair: str
    side: str
    score: float
    revision: int
    features: dict[str, Any]
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CandidateReview:
    pair: str
    decision: str
    confidence: int
    stop_loss: float | None
    take_profit: float | None
    quantity_suggestion: float | None
    rationale: str


@dataclass(frozen=True, slots=True)
class TradeProposal:
    pair: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit: float
    quantity: float
    leverage: int
    margin_currency: str = "INR"


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    quantity: float = 0.0
    leverage: int = 0
    reason: str = ""
