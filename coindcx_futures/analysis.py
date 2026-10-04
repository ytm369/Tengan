from __future__ import annotations

import asyncio
import math
import statistics
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

from .bus import EventBus
from .config import Settings
from .state import MarketState
from .types import Candidate, MarketEvent, MarketSnapshot, NodeResult


@dataclass(frozen=True, slots=True)
class Node:
    name: str
    calculate: Callable[[MarketEvent, MarketSnapshot | None, tuple[MarketSnapshot, ...]], NodeResult | None]


class AnalysisPipeline:
    """Runs independent analysis subscribers concurrently over the same event stream."""

    def __init__(self, state: MarketState, market_bus: EventBus[MarketEvent], settings: Settings,
                 result_callback: Callable[[NodeResult], None] | None = None,
                 cycle_callback: Callable[[dict], None] | None = None) -> None:
        self.state = state
        self.market_bus = market_bus
        self.settings = settings
        self.result_callback = result_callback
        self.cycle_callback = cycle_callback
        self.results: dict[str, dict[str, NodeResult]] = defaultdict(dict)
        self.global_results: dict[str, NodeResult] = {}
        self.result_queue: asyncio.Queue[NodeResult] = asyncio.Queue(maxsize=4096)
        self.nodes = self._build_nodes()
        # Subscribe before the engine starts ingesting bootstrap snapshots. Otherwise
        # an early event can be published before the node tasks get their first turn.
        self._subscriptions = {node.name: self.market_bus.subscribe() for node in self.nodes}

    def _build_nodes(self) -> tuple[Node, ...]:
        return (
            Node("btc_regime", self._btc_regime),
            Node("liquidity", self._liquidity),
            Node("momentum", self._momentum),
            Node("trend", self._trend),
            Node("relative_strength", self._relative_strength),
            Node("funding_positioning", self._funding_positioning),
        )

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self._run_node(node, self._subscriptions[node.name]), name=f"analysis-{node.name}")
            for node in self.nodes
        ]
        tasks.append(asyncio.create_task(self._collect_results(), name="analysis-results"))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for subscription in self._subscriptions.values():
                self.market_bus.unsubscribe(subscription)
            self._subscriptions.clear()

    async def candidate_loop(self, output: asyncio.Queue[tuple[Candidate, ...]]) -> None:
        while True:
            await asyncio.sleep(self.settings.review_interval_seconds)
            snapshots = await self.state.snapshots()
            now_ms = int(time.time() * 1000)
            candidates = self.select_candidates(snapshots, now_ms)
            fresh_tickers = sum(
                _is_fresh(snapshot.field_timestamps.get("ticker", 0), now_ms, self.settings.stale_after_seconds)
                and snapshot.price is not None and math.isfinite(snapshot.price) and snapshot.price > 0
                for snapshot in snapshots
            )
            if self.cycle_callback:
                self.cycle_callback({
                    "timestamp_ms": now_ms,
                    "snapshot_count": len(snapshots),
                    "fresh_ticker_count": fresh_tickers,
                    "candidate_count": len(candidates),
                    "candidates": [
                        {
                            "pair": item.pair, "side": item.side, "score": item.score,
                            "revision": item.revision, "features": item.features,
                            "reasons": item.reasons,
                        }
                        for item in candidates
                    ],
                })
            if output.full():
                try:
                    output.get_nowait()
                    output.task_done()
                except asyncio.QueueEmpty:
                    pass
            try:
                output.put_nowait(candidates)
            except asyncio.QueueFull:
                pass

    def select_candidates(self, snapshots: tuple[MarketSnapshot, ...], now_ms: int) -> tuple[Candidate, ...]:
        fresh = {
            snapshot.pair: snapshot
            for snapshot in snapshots
            if _is_fresh(snapshot.field_timestamps.get("ticker", 0), now_ms, self.settings.stale_after_seconds)
            and snapshot.price is not None and math.isfinite(snapshot.price) and snapshot.price > 0
        }
        if not fresh:
            return ()
        liquidities = self.results.get("liquidity", {})
        directional_weights = {
            "momentum": self.settings.weights.get("momentum", 0),
            "trend": self.settings.weights.get("trend", 0),
            "relative_strength": self.settings.weights.get("relative_strength", 0),
            "btc_regime": self.settings.weights.get("btc_regime", 0),
            "funding_positioning": self.settings.weights.get("funding", 0),
        }
        denom = sum(directional_weights.values())
        if denom <= 0:
            return ()
        output: list[Candidate] = []
        for pair, snapshot in fresh.items():
            if "BTC" in pair:
                continue
            features: dict[str, float | None] = {}
            reasons: list[str] = []
            weighted = 0.0
            used_weight = 0.0
            for name, weight in directional_weights.items():
                result = self.results.get(name, {}).get(pair)
                if name == "btc_regime":
                    result = self.global_results.get("btc_regime")
                if result is not None and not _is_fresh(result.timestamp_ms, now_ms, self.settings.stale_after_seconds):
                    result = None
                if result is None or result.score is None or not result.available:
                    features[name] = None
                    continue
                weighted += result.score * weight
                used_weight += weight
                features[name] = result.score
                if result.features.get("reason"):
                    reasons.append(str(result.features["reason"]))
            liquidity = liquidities.get(pair)
            if liquidity is not None and not _is_fresh(
                liquidity.timestamp_ms, now_ms, self.settings.stale_after_seconds
            ):
                liquidity = None
            liquidity_score = liquidity.score if liquidity and liquidity.available and liquidity.score is not None else None
            features["liquidity"] = liquidity_score
            if used_weight == 0 or liquidity_score is None:
                continue
            if liquidity_score < 0.15:
                continue
            direction = max(-1.0, min(1.0, weighted / used_weight))
            quality = max(0.0, min(1.0, liquidity_score))
            strength = abs(direction) * quality
            if strength < self.settings.minimum_candidate_score:
                continue
            side = "long" if direction > 0 else "short"
            output.append(Candidate(
                pair=pair, side=side, score=strength, revision=snapshot.revision,
                features={
                    **features,
                    "price": snapshot.price,
                    "mark_price": snapshot.mark_price,
                    "volume_24h": snapshot.volume_24h,
                    "change_24h_pct": snapshot.change_24h_pct,
                    "funding_rate": snapshot.funding_rate,
                    "candles": {tf: [c.close for c in values[-8:]] for tf, values in snapshot.candles.items()},
                    "open_interest": None,
                },
                reasons=tuple(reasons),
            ))
        output.sort(key=lambda candidate: candidate.score, reverse=True)
        # Limit each direction independently. Ranker emits only the stronger side for each instrument.
        longs = [candidate for candidate in output if candidate.side == "long"][:self.settings.candidate_count_per_side]
        shorts = [candidate for candidate in output if candidate.side == "short"][:self.settings.candidate_count_per_side]
        return tuple(longs + shorts)

    async def _run_node(self, node: Node, subscription) -> None:
        try:
            while True:
                event = await subscription.queue.get()
                try:
                    snapshot = await self.state.snapshot(event.pair)
                    all_snapshots = await self.state.snapshots() if node.name == "relative_strength" else ()
                    result = node.calculate(event, snapshot, all_snapshots)
                    if result is not None:
                        try:
                            self.result_queue.put_nowait(result)
                        except asyncio.QueueFull:
                            # Preserve recency under overload: discard one queued stale calculation.
                            self.result_queue.get_nowait()
                            self.result_queue.task_done()
                            self.result_queue.put_nowait(result)
                finally:
                    subscription.queue.task_done()
        finally:
            self.market_bus.unsubscribe(subscription)

    async def _collect_results(self) -> None:
        while True:
            result = await self.result_queue.get()
            try:
                if result.pair == "GLOBAL":
                    previous = self.global_results.get(result.node)
                    if previous is None or result.timestamp_ms >= previous.timestamp_ms:
                        self.global_results[result.node] = result
                else:
                    previous = self.results[result.node].get(result.pair)
                    if previous is None or result.input_revision >= previous.input_revision:
                        self.results[result.node][result.pair] = result
                if self.result_callback:
                    self.result_callback(result)
            finally:
                self.result_queue.task_done()

    def _btc_regime(self, event, snapshot, _all) -> NodeResult | None:
        if not event.pair.startswith("B-BTC_") or event.kind not in {"ticker", "candle"} or snapshot is None:
            return None
        score = _normalize_pct(snapshot.change_24h_pct)
        if score is None:
            return None
        return NodeResult("btc_regime", "GLOBAL", snapshot.revision, event.timestamp_ms, score,
                          {"btc_pair": event.pair, "reason": f"BTC 24h change {snapshot.change_24h_pct:.2f}%"})

    def _liquidity(self, event, snapshot, _all) -> NodeResult | None:
        if snapshot is None or event.kind not in {"ticker", "orderbook"}:
            return None
        try:
            bids = sorted(
                ((float(price), float(qty)) for price, qty in snapshot.bids.items()
                 if _valid_level(price, qty)), reverse=True
            )
            asks = sorted(
                ((float(price), float(qty)) for price, qty in snapshot.asks.items()
                 if _valid_level(price, qty))
            )
            if not bids or not asks:
                return NodeResult("liquidity", event.pair, snapshot.revision, event.timestamp_ms, None,
                                  {"reason": "order book not available"}, available=False)
            best_bid, best_ask = bids[0][0], asks[0][0]
            mid = (best_bid + best_ask) / 2
            spread_pct = ((best_ask - best_bid) / mid * 100) if mid > 0 else 100.0
            bid_depth = sum(price * quantity for price, quantity in bids[:5])
            ask_depth = sum(price * quantity for price, quantity in asks[:5])
            depth_total = bid_depth + ask_depth
            imbalance = (bid_depth - ask_depth) / depth_total if depth_total else 0.0
            spread_quality = max(0.0, min(1.0, 1 - spread_pct / 0.5))
            depth_quality = max(0.0, min(1.0, math.log10(max(depth_total, 1)) / 8))
            score = 0.7 * spread_quality + 0.3 * depth_quality
            return NodeResult("liquidity", event.pair, snapshot.revision, event.timestamp_ms, score,
                              {"spread_pct": spread_pct, "depth_imbalance": imbalance, "reason": "spread and top-five depth"})
        except (ValueError, ZeroDivisionError):
            return NodeResult("liquidity", event.pair, snapshot.revision, event.timestamp_ms, None,
                              {"reason": "invalid orderbook"}, available=False)

    def _momentum(self, event, snapshot, _all) -> NodeResult | None:
        if snapshot is None or event.kind != "ticker":
            return None
        score = _normalize_pct(snapshot.change_24h_pct)
        if score is None:
            return None
        return NodeResult("momentum", event.pair, snapshot.revision, event.timestamp_ms, score,
                          {"reason": f"24h price change {snapshot.change_24h_pct:.2f}%"})

    def _trend(self, event, snapshot, _all) -> NodeResult | None:
        if snapshot is None or event.kind not in {"candle", "ticker"}:
            return None
        returns: list[float] = []
        for timeframe in self.settings.timeframes:
            closes = [c.close for c in snapshot.candles.get(timeframe, ()) if c.close > 0]
            if len(closes) < 2:
                continue
            base = closes[max(0, len(closes) - 8)]
            returns.append(_normalize_return((closes[-1] - base) / base * 100))
        if not returns:
            return NodeResult("trend", event.pair, snapshot.revision, event.timestamp_ms, None,
                              {"reason": "warming up candle history"}, available=False)
        score = sum(returns) / len(returns)
        return NodeResult("trend", event.pair, snapshot.revision, event.timestamp_ms, score,
                          {"timeframes_used": len(returns), "reason": "multi-timeframe candle trend"})

    def _relative_strength(self, event, snapshot, all_snapshots) -> NodeResult | None:
        if snapshot is None or event.kind != "ticker" or snapshot.change_24h_pct is None:
            return None
        changes = [item.change_24h_pct for item in all_snapshots if item.change_24h_pct is not None]
        if len(changes) < 2:
            return NodeResult("relative_strength", event.pair, snapshot.revision, event.timestamp_ms, None,
                              {"reason": "waiting for comparison universe"}, available=False)
        median = statistics.median(changes)
        relative = snapshot.change_24h_pct - median
        return NodeResult("relative_strength", event.pair, snapshot.revision, event.timestamp_ms,
                          _normalize_return(relative), {"vs_universe_median_pct": relative, "reason": "24h return vs universe median"})

    def _funding_positioning(self, event, snapshot, _all) -> NodeResult | None:
        if snapshot is None or event.kind != "ticker":
            return None
        funding_timestamp = snapshot.field_timestamps.get("funding_rate", 0)
        if snapshot.funding_rate is None or event.timestamp_ms - funding_timestamp > self.settings.stale_after_seconds * 1000:
            return NodeResult("funding_positioning", event.pair, snapshot.revision, event.timestamp_ms, None,
                              {"reason": "funding rate unavailable; positioning and open interest omitted"}, available=False)
        # Positive funding means longs pay shorts; fade the more crowded side modestly.
        score = max(-1.0, min(1.0, -snapshot.funding_rate / 0.001))
        return NodeResult("funding_positioning", event.pair, snapshot.revision, event.timestamp_ms, score,
                          {"funding_rate": snapshot.funding_rate, "reason": "funding rate crowding adjustment"})


def _normalize_pct(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return _normalize_return(value)


def _normalize_return(pct: float) -> float:
    return max(-1.0, min(1.0, math.tanh(pct / 5.0)))


def _is_fresh(timestamp_ms: int, now_ms: int, max_age_seconds: float) -> bool:
    age_ms = now_ms - timestamp_ms
    return -5_000 <= age_ms <= max_age_seconds * 1000


def _valid_level(price, quantity) -> bool:
    try:
        return math.isfinite(float(price)) and float(price) > 0 and math.isfinite(float(quantity)) and float(quantity) > 0
    except (TypeError, ValueError):
        return False
