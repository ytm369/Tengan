from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import asdict, replace
from pathlib import Path

from .analysis import AnalysisPipeline
from .bus import EventBus
from .coindcx import CoinDCXPrivateClient, CoinDCXPublicClient, CoinDCXSocketFeed
from .config import Settings, load_dotenv, load_settings
from .execution import LiveExecutor
from .gemini import GeminiError, GeminiReviewer, GeminiUnavailable
from .paper import PaperOutcomeTracker
from .state import MarketState
from .storage import AuditStore
from .types import Candidate, CandidateReview, MarketEvent

LOG = logging.getLogger(__name__)


class CoinDCXFramework:
    def __init__(self, config_path: Path, audit_path: Path, *, arm_live: bool = False) -> None:
        load_dotenv(config_path.parent / ".env")
        self.config_path = config_path
        self.settings = load_settings(config_path)
        self.arm_live = arm_live
        if arm_live and self.settings.execution_mode != "live":
            raise ValueError("Set execution.mode='live' in config.toml as well as passing --arm-live")
        if arm_live and not self.settings.risk_limits.configured():
            raise ValueError("Live mode remains disabled until every hard risk limit is configured")
        if arm_live and (not os.getenv("COINDCX_API_KEY") or not os.getenv("COINDCX_API_SECRET")):
            raise ValueError("Live mode requires COINDCX_API_KEY and COINDCX_API_SECRET in .env")
        self.store = AuditStore(audit_path)
        self.paper_tracker = None if arm_live else PaperOutcomeTracker(self.store)
        self.public = CoinDCXPublicClient(self.settings.rest_timeout_seconds)
        self.private = None
        if arm_live:
            self.private = CoinDCXPrivateClient(
                os.environ["COINDCX_API_KEY"], os.environ["COINDCX_API_SECRET"],
                timeout=self.settings.rest_timeout_seconds,
            )
        self.state = MarketState()
        self.market_bus: EventBus[MarketEvent] = EventBus(queue_size=4096)
        self.ingest_queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=10000)
        self.candidates: asyncio.Queue[tuple[Candidate, ...]] = asyncio.Queue(maxsize=1)
        self.pipeline = AnalysisPipeline(
            self.state, self.market_bus, self.settings, self.store.log_node_result,
            self._log_candidate_cycle,
        )
        self.reviewer = GeminiReviewer(
            self.settings.model_name,
            timeout=self.settings.model_timeout_seconds,
        )
        self.executor = None
        if self.private:
            self.executor = LiveExecutor(self.private, self.public, self.state, self.store, self.settings)
        self._config_mtime_ns: int | None = None
        if self.config_path.exists():
            self._config_mtime_ns = self.config_path.stat().st_mtime_ns
        self._config_version = 0

    async def run(self) -> None:
        pairs, initial_prices = await self._discover_universe()
        if not pairs:
            raise RuntimeError("CoinDCX returned no active INR futures instruments")
        LOG.info("Watching %d active INR futures instruments", len(pairs))
        feed = CoinDCXSocketFeed(self._enqueue, set(pairs), self.settings.timeframes)
        tasks = [
            asyncio.create_task(self._ingest_loop(), name="market-ingest"),
            asyncio.create_task(self.pipeline.run(), name="analysis-pipeline"),
            asyncio.create_task(self.pipeline.candidate_loop(self.candidates), name="candidate-ranker"),
            asyncio.create_task(self._review_loop(), name="gemini-reviewer"),
            asyncio.create_task(self._watch_config(), name="config-watcher"),
            asyncio.create_task(self._refresh_universe(feed), name="instrument-discovery"),
            asyncio.create_task(self._poll_orderbooks(feed), name="orderbook-rest-fallback"),
            asyncio.create_task(self._bootstrap_candles(pairs), name="candle-history-bootstrap"),
        ]
        await asyncio.sleep(0)
        for pair in pairs:
            ticker = initial_prices.get(pair)
            if ticker:
                await self._enqueue(MarketEvent(
                    pair=pair,
                    kind="ticker",
                    timestamp_ms=int(time.time() * 1000),
                    source="coindcx_rest_bootstrap",
                    payload=ticker,
                ))
        await self.ingest_queue.join()
        LOG.info("Paper mode is active" if not self.arm_live else "LIVE MODE ARMED; deterministic risk gate remains mandatory")
        try:
            await feed.run()
        finally:
            await feed.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.store.close()

    async def _discover_universe(self) -> tuple[list[str], dict[str, dict]]:
        active = await self.public.get_active_instruments("INR")
        prices = await self.public.get_current_prices()
        eligible = set(active)
        ranked = sorted(
            (pair for pair in active),
            key=lambda pair: _number(prices.get(pair, {}).get("v")),
            reverse=True,
        )
        selected = ranked[:self.settings.max_symbols]
        btc = next((pair for pair in active if pair.startswith("B-BTC_")), None)
        if btc and btc not in selected:
            selected = (selected[:-1] + [btc]) if selected else [btc]
        selected = list(dict.fromkeys(pair for pair in selected if pair in eligible))
        return selected, prices

    async def _refresh_universe(self, feed: CoinDCXSocketFeed) -> None:
        while True:
            await asyncio.sleep(self.settings.instrument_refresh_seconds)
            try:
                pairs, prices = await self._discover_universe()
                old_pairs = set(feed.pairs)
                await feed.set_pairs(set(pairs))
                for pair in set(pairs) - old_pairs:
                    ticker = prices.get(pair)
                    if ticker:
                        await self._enqueue(MarketEvent(
                            pair=pair, kind="ticker", timestamp_ms=int(time.time() * 1000),
                            source="coindcx_rest_universe_refresh", payload=ticker,
                        ))
                LOG.info("Instrument universe refreshed: %d active INR contracts", len(pairs))
            except Exception:
                LOG.exception("Instrument discovery refresh failed; keeping the existing universe")

    async def _poll_orderbooks(self, feed: CoinDCXSocketFeed) -> None:
        semaphore = asyncio.Semaphore(8)

        async def fetch(pair: str) -> None:
            async with semaphore:
                try:
                    book = await self.public.get_orderbook(pair, depth=50)
                    timestamp = int(book.get("ts", time.time() * 1000))
                    await self._enqueue(MarketEvent(
                        pair=pair, kind="orderbook", timestamp_ms=timestamp,
                        source="coindcx_rest_orderbook", source_sequence=_optional_int(book.get("vs")),
                        payload=book,
                    ))
                except Exception as exc:
                    LOG.debug("Order book refresh failed for %s: %s", pair, exc)

        while True:
            started = time.monotonic()
            await asyncio.gather(*(fetch(pair) for pair in sorted(feed.pairs)))
            delay = self.settings.orderbook_poll_seconds - (time.monotonic() - started)
            if delay > 0:
                await asyncio.sleep(delay)

    async def _bootstrap_candles(self, pairs: list[str]) -> None:
        resolution_by_timeframe = {"1m": "1", "5m": "5", "1h": "60", "1d": "1D"}
        timeframe = next((tf for tf in ("1m", "5m", "1h", "1d")
                          if tf in self.settings.timeframes and tf in resolution_by_timeframe), None)
        if timeframe is None:
            return
        resolution = resolution_by_timeframe[timeframe]
        duration_ms = {"1m": 60_000, "5m": 300_000, "1h": 3_600_000, "1d": 86_400_000}[timeframe]
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - duration_ms * 64
        semaphore = asyncio.Semaphore(6)

        async def fetch(pair: str) -> None:
            async with semaphore:
                try:
                    candles = await self.public.get_candles(pair, resolution, start_ms, now_ms)
                    for candle in candles:
                        raw_time = int(candle.get("time", candle.get("open_time", 0)))
                        timestamp_ms = raw_time if raw_time >= 10_000_000_000 else raw_time * 1000
                        await self._enqueue(MarketEvent(
                            pair=pair, kind="candle", timestamp_ms=timestamp_ms,
                            source="coindcx_rest_candle_history", payload=candle, timeframe=timeframe,
                        ))
                except Exception as exc:
                    LOG.debug("Candle history bootstrap failed for %s: %s", pair, exc)

        await asyncio.gather(*(fetch(pair) for pair in pairs))

    async def _enqueue(self, event: MarketEvent) -> None:
        if self.ingest_queue.full():
            try:
                self.ingest_queue.get_nowait()
                self.ingest_queue.task_done()
            except asyncio.QueueEmpty:
                pass
        try:
            self.ingest_queue.put_nowait(event)
        except asyncio.QueueFull:
            LOG.warning("Market ingest queue saturated; dropping latest event")

    async def _ingest_loop(self) -> None:
        while True:
            event = await self.ingest_queue.get()
            try:
                if await self.state.apply(event):
                    if self.paper_tracker and event.kind == "ticker":
                        snapshot = await self.state.snapshot(event.pair)
                        self.paper_tracker.on_market_event(event, snapshot.price if snapshot else None)
                    self.store.log_event(event)
                    self.market_bus.publish(event)
            except Exception:
                LOG.exception("Could not ingest market event for %s", event.pair)
            finally:
                self.ingest_queue.task_done()

    async def _review_loop(self) -> None:
        last_request = 0.0
        while True:
            candidates = await self.candidates.get()
            try:
                if not candidates:
                    LOG.info("No fresh candidates this review cycle")
                    continue
                candidates = tuple(replace(
                    candidate,
                    features={
                        **candidate.features,
                        "recent_paper_outcomes": self.store.recent_paper_outcomes(candidate.pair),
                    },
                ) for candidate in candidates)
                self._log_candidates(candidates)
                if not self.settings.model_enabled:
                    LOG.warning("Gemini analysis is disabled; no entries will be proposed")
                    continue
                minimum_interval = 60.0 / max(1, self.settings.maximum_model_requests_per_minute)
                delay = minimum_interval - (time.monotonic() - last_request)
                if delay > 0:
                    await asyncio.sleep(delay)
                last_request = time.monotonic()
                try:
                    reviews = await self.reviewer.review(candidates)
                except (GeminiUnavailable, GeminiError) as exc:
                    self.store.log_model_review(int(time.time() * 1000), {
                        "status": "unavailable", "reason": str(exc),
                        "candidate_pairs": [candidate.pair for candidate in candidates],
                    })
                    LOG.error("Model review failed closed: %s", exc)
                    continue
                except Exception as exc:
                    self.store.log_model_review(int(time.time() * 1000), {
                        "status": "error", "reason": str(exc),
                        "candidate_pairs": [candidate.pair for candidate in candidates],
                    })
                    LOG.exception("Unexpected model error; no entry will be sent")
                    continue
                timestamp = int(time.time() * 1000)
                self.store.log_model_review(timestamp, {"status": "success", "reviews": reviews})
                await self._handle_reviews(candidates, reviews)
            finally:
                self.candidates.task_done()

    async def _handle_reviews(self, candidates: tuple[Candidate, ...], reviews: tuple[CandidateReview, ...]) -> None:
        candidate_by_pair = {candidate.pair: candidate for candidate in candidates}
        for review in reviews:
            candidate = candidate_by_pair.get(review.pair)
            if candidate is None or review.decision == "skip":
                continue
            if review.confidence < self.settings.minimum_model_confidence:
                continue
            snapshot = await self.state.snapshot(candidate.pair)
            if snapshot is None or snapshot.price is None:
                continue
            now_ms = int(time.time() * 1000)
            ticker_timestamp = snapshot.field_timestamps.get("ticker", 0)
            age_ms = now_ms - ticker_timestamp
            if age_ms < -5_000 or age_ms > self.settings.max_entry_age_seconds * 1000:
                self.store.log_trade(now_ms, "paper_signal_rejected", {
                    "pair": candidate.pair, "reason": "market data became stale during model review",
                })
                continue
            if not self.arm_live:
                opened = bool(self.paper_tracker and self.paper_tracker.open(
                    pair=candidate.pair, side=candidate.side, entry_price=float(snapshot.price),
                    stop_loss=review.stop_loss, take_profit=review.take_profit,
                    timestamp_ms=ticker_timestamp, confidence=review.confidence,
                    score=candidate.score, rationale=review.rationale,
                ))
                if opened:
                    LOG.info("PAPER signal: %s %s confidence=%d", candidate.side, candidate.pair, review.confidence)
                else:
                    self.store.log_trade(int(time.time() * 1000), "paper_signal_rejected", {
                        "pair": candidate.pair,
                        "reason": "invalid protection levels or an existing simulated position is open",
                    })
                continue
            if self.executor is None:
                LOG.error("Live executor is unavailable; no order sent")
                continue
            await self.executor.execute(candidate, review)

    def _log_candidates(self, candidates: tuple[Candidate, ...]) -> None:
        summary = ", ".join(f"{item.side.upper()} {item.pair} {item.score:.3f}" for item in candidates)
        LOG.info("Shortlist: %s", summary)

    def _log_candidate_cycle(self, payload: dict) -> None:
        self.store.log_trade(int(payload["timestamp_ms"]), "candidate_cycle", payload)

    async def _watch_config(self) -> None:
        while True:
            await asyncio.sleep(2)
            if not self.config_path.exists():
                continue
            mtime = self.config_path.stat().st_mtime_ns
            if mtime == self._config_mtime_ns:
                continue
            try:
                updated = load_settings(self.config_path)
                if updated.timeframes != self.settings.timeframes or updated.max_symbols != self.settings.max_symbols:
                    raise ValueError("market.timeframes/max_symbols changes require a restart")
                if self.arm_live and (updated.risk_limits != self.settings.risk_limits or updated.execution_mode != "live"):
                    raise ValueError("live execution/risk changes require a restart")
                self.settings = updated
                self.pipeline.settings = updated
                self.reviewer.model = updated.model_name
                self.reviewer.timeout = updated.model_timeout_seconds
                if self.executor:
                    self.executor.settings = updated
                self._config_mtime_ns = mtime
                self._config_version += 1
                self.store.log_config(int(time.time() * 1000), self._config_version, asdict(updated))
                LOG.info("Applied strategy configuration version %d", self._config_version)
            except Exception as exc:
                LOG.error("Configuration reload rejected; continuing with prior settings: %s", exc)
                self._config_mtime_ns = mtime


def _number(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _optional_int(value) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
