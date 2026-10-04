from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

from .analysis import AnalysisPipeline
from .bus import EventBus
from .config import Settings
from .state import MarketState
from .types import Candidate, MarketEvent


async def replay_market_events(path: Path, settings: Settings) -> tuple[int, tuple[Candidate, ...]]:
    """Replay recorded public market events through the scanner without network or orders."""
    state = MarketState()
    bus: EventBus[MarketEvent] = EventBus(queue_size=4096)
    pipeline = AnalysisPipeline(state, bus, settings)
    task = asyncio.create_task(pipeline.run(), name="paper-replay-analysis")
    event_count = 0
    latest_ticker_ms = 0
    try:
        await asyncio.sleep(0)
        for event in _read_market_events(path):
            if not await state.apply(event):
                continue
            bus.publish(event)
            event_count += 1
            if event.kind == "ticker":
                latest_ticker_ms = max(latest_ticker_ms, event.timestamp_ms)
            await asyncio.gather(*(sub.queue.join() for sub in pipeline._subscriptions.values()))
            await pipeline.result_queue.join()
        if event_count == 0 or latest_ticker_ms == 0:
            return event_count, ()
        candidates = pipeline.select_candidates(await state.snapshots(), latest_ticker_ms)
        return event_count, candidates
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def _read_market_events(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Audit database not found: {path}")
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    try:
        cursor = connection.execute(
            "SELECT pair,kind,timestamp_ms,source,timeframe,payload FROM market_events ORDER BY id"
        )
        for pair, kind, timestamp_ms, source, timeframe, payload in cursor:
            yield MarketEvent(
                pair=pair, kind=kind, timestamp_ms=int(timestamp_ms), source=source,
                timeframe=timeframe, payload=json.loads(payload),
            )
    finally:
        connection.close()
