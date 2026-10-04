from __future__ import annotations

import os
import tomllib
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class Settings:
    margin_currency: str = "INR"
    max_symbols: int = 30
    timeframes: tuple[str, ...] = ("1m", "5m", "15m", "1h")
    stale_after_seconds: float = 20.0
    instrument_refresh_seconds: float = 900.0
    rest_timeout_seconds: float = 10.0
    orderbook_poll_seconds: float = 10.0
    candidate_count_per_side: int = 3
    review_interval_seconds: float = 60.0
    minimum_candidate_score: float = 0.15
    minimum_model_confidence: int = 65
    maximum_model_requests_per_minute: int = 1
    model_enabled: bool = True
    model_provider: str = "gemini-developer-api"
    model_name: str = "gemini-3.1-flash-lite"
    model_timeout_seconds: float = 20.0
    execution_mode: str = "paper"
    margin_type: str = "isolated"
    max_entry_age_seconds: float = 5.0
    risk_limits: RiskLimits = field(default_factory=lambda: RiskLimits())
    weights: dict[str, float] = field(default_factory=lambda: {
        "momentum": 0.25,
        "trend": 0.25,
        "relative_strength": 0.20,
        "liquidity": 0.15,
        "btc_regime": 0.10,
        "funding": 0.05,
    })

    def validate(self) -> None:
        if self.margin_currency != "INR":
            raise ValueError("This version supports INR-margined futures only")
        if self.max_symbols < 1:
            raise ValueError("market.max_symbols must be at least 1")
        if self.orderbook_poll_seconds <= 0:
            raise ValueError("market.orderbook_poll_seconds must be positive")
        if not self.timeframes:
            raise ValueError("At least one candle timeframe is required")
        allowed_timeframes = {"1m", "5m", "15m", "30m", "1h", "4h", "8h", "1d", "3d", "1w", "1M"}
        if any(timeframe not in allowed_timeframes for timeframe in self.timeframes):
            raise ValueError("market.timeframes contains an unsupported CoinDCX futures interval")
        positive_settings = {
            "stale_after_seconds": self.stale_after_seconds,
            "instrument_refresh_seconds": self.instrument_refresh_seconds,
            "rest_timeout_seconds": self.rest_timeout_seconds,
            "orderbook_poll_seconds": self.orderbook_poll_seconds,
            "review_interval_seconds": self.review_interval_seconds,
            "model_timeout_seconds": self.model_timeout_seconds,
            "max_entry_age_seconds": self.max_entry_age_seconds,
        }
        if any(not math.isfinite(value) or value <= 0 for value in positive_settings.values()):
            raise ValueError("All polling intervals and timeouts must be positive finite numbers")
        if not 0 <= self.minimum_candidate_score <= 1:
            raise ValueError("scanner.minimum_candidate_score must be within 0..1")
        if not 0 <= self.minimum_model_confidence <= 100:
            raise ValueError("scanner.minimum_model_confidence must be within 0..100")
        if self.execution_mode not in {"paper", "live"}:
            raise ValueError("execution.mode must be 'paper' or 'live'")
        if self.margin_type != "isolated":
            raise ValueError("INR-margined CoinDCX futures are configured as isolated")
        if self.model_provider != "gemini-developer-api":
            raise ValueError("Only the direct Gemini Developer API adapter is supported")
        if self.candidate_count_per_side != 3:
            raise ValueError("candidate_count_per_side is fixed at 3 for this version")
        if self.maximum_model_requests_per_minute < 1:
            raise ValueError("maximum_model_requests_per_minute must be positive")
        supported_weights = {"momentum", "trend", "relative_strength", "liquidity", "btc_regime", "funding"}
        if set(self.weights) - supported_weights:
            raise ValueError("Analysis weights contain an unsupported node")
        if any(not math.isfinite(value) or value < 0 for value in self.weights.values()):
            raise ValueError("Analysis weights must be nonnegative finite numbers")
        if not any(self.weights.values()):
            raise ValueError("At least one analysis weight must be positive")


@dataclass(frozen=True, slots=True)
class RiskLimits:
    max_risk_per_trade_inr: float = 0.0
    max_daily_loss_inr: float = 0.0
    max_total_exposure_inr: float = 0.0
    max_open_positions: int = 0
    max_leverage: int = 0

    def configured(self) -> bool:
        return all((
            math.isfinite(self.max_risk_per_trade_inr) and self.max_risk_per_trade_inr > 0,
            math.isfinite(self.max_daily_loss_inr) and self.max_daily_loss_inr > 0,
            math.isfinite(self.max_total_exposure_inr) and self.max_total_exposure_inr > 0,
            self.max_open_positions > 0,
            self.max_leverage > 0,
        ))


def load_dotenv(path: Path = Path(".env")) -> None:
    """Load simple KEY=value entries without overriding process environment."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key:
            os.environ.setdefault(key, value)


def load_settings(path: Path) -> Settings:
    if not path.exists():
        settings = Settings()
        settings.validate()
        return settings
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    market = document.get("market", {})
    scanner = document.get("scanner", {})
    model = document.get("model", {})
    execution = document.get("execution", {})
    risk = document.get("risk", {})
    analysis = document.get("analysis", {}).get("weights", {})
    defaults = Settings()
    limits = RiskLimits(
        max_risk_per_trade_inr=float(risk.get("max_risk_per_trade_inr", 0) or 0),
        max_daily_loss_inr=float(risk.get("max_daily_loss_inr", 0) or 0),
        max_total_exposure_inr=float(risk.get("max_total_exposure_inr", 0) or 0),
        max_open_positions=int(risk.get("max_open_positions", 0) or 0),
        max_leverage=int(risk.get("max_leverage", 0) or 0),
    )
    merged_weights = dict(defaults.weights)
    merged_weights.update({key: float(value) for key, value in analysis.items()})
    settings = Settings(
        margin_currency=str(market.get("margin_currency", defaults.margin_currency)).upper(),
        max_symbols=int(market.get("max_symbols", defaults.max_symbols)),
        timeframes=tuple(market.get("timeframes", defaults.timeframes)),
        stale_after_seconds=float(market.get("stale_after_seconds", defaults.stale_after_seconds)),
        instrument_refresh_seconds=float(market.get("instrument_refresh_seconds", defaults.instrument_refresh_seconds)),
        rest_timeout_seconds=float(market.get("rest_timeout_seconds", defaults.rest_timeout_seconds)),
        orderbook_poll_seconds=float(market.get("orderbook_poll_seconds", defaults.orderbook_poll_seconds)),
        candidate_count_per_side=int(scanner.get("candidate_count_per_side", defaults.candidate_count_per_side)),
        review_interval_seconds=float(scanner.get("review_interval_seconds", defaults.review_interval_seconds)),
        minimum_candidate_score=float(scanner.get("minimum_candidate_score", defaults.minimum_candidate_score)),
        minimum_model_confidence=int(scanner.get("minimum_model_confidence", defaults.minimum_model_confidence)),
        maximum_model_requests_per_minute=int(scanner.get("maximum_model_requests_per_minute", defaults.maximum_model_requests_per_minute)),
        model_enabled=bool(model.get("enabled", defaults.model_enabled)),
        model_provider=str(model.get("provider", defaults.model_provider)),
        model_name=str(model.get("model", defaults.model_name)),
        model_timeout_seconds=float(model.get("timeout_seconds", defaults.model_timeout_seconds)),
        execution_mode=str(execution.get("mode", defaults.execution_mode)).lower(),
        margin_type=str(execution.get("margin_type", defaults.margin_type)).lower(),
        max_entry_age_seconds=float(execution.get("max_entry_age_seconds", defaults.max_entry_age_seconds)),
        risk_limits=limits,
        weights=merged_weights,
    )
    settings.validate()
    return settings
