from __future__ import annotations

import asyncio
import math
from collections import defaultdict, deque

from .types import Candle, MarketEvent, MarketSnapshot


class MarketState:
    def __init__(self, candle_limit: int = 256) -> None:
        self._lock = asyncio.Lock()
        self._items: dict[str, dict] = {}
        self._seen: dict[tuple[str, str, str | None], tuple[int, int | None]] = {}
        self._revision = defaultdict(int)
        self._candle_limit = candle_limit

    async def apply(self, event: MarketEvent) -> bool:
        key = (event.pair, event.kind, event.timeframe)
        async with self._lock:
            previous = self._seen.get(key)
            incoming = (event.timestamp_ms, event.source_sequence)
            if previous is not None:
                if incoming[0] < previous[0]:
                    return False
                if incoming[0] == previous[0] and incoming[1] is not None and previous[1] is not None and incoming[1] <= previous[1]:
                    return False
            self._seen[key] = incoming
            item = self._items.setdefault(event.pair, {
                "price": None, "mark_price": None, "volume_24h": None,
                "change_24h_pct": None, "funding_rate": None,
                "bids": {}, "asks": {}, "candles": defaultdict(lambda: deque(maxlen=self._candle_limit)),
                "long_percent": None, "short_percent": None, "open_interest": None,
                "updated_at_ms": 0, "field_timestamps": {},
            })
            payload = event.payload
            if event.kind == "ticker":
                mapping = {
                    "price": ("ls", "price", "p"),
                    "mark_price": ("mp", "mark_price"),
                    "volume_24h": ("v", "volume_24h"),
                    "change_24h_pct": ("pc", "change_24h_pct"),
                    "funding_rate": ("fr", "efr", "funding_rate"),
                }
                for field, sources in mapping.items():
                    value = _first(payload, *sources)
                    if value is not None:
                        item[field] = _float(value)
                        item["field_timestamps"][field] = event.timestamp_ms
            elif event.kind == "orderbook":
                item["bids"] = dict(payload.get("bids") or {})
                item["asks"] = dict(payload.get("asks") or {})
            elif event.kind == "candle":
                timeframe = event.timeframe or str(payload.get("i", ""))
                candle = _parse_candle(payload)
                if timeframe and candle:
                    candles = item["candles"][timeframe]
                    if candles and candle.open_time_ms < candles[-1].open_time_ms:
                        return False
                    if candles and candle.open_time_ms == candles[-1].open_time_ms:
                        candles[-1] = candle
                    else:
                        candles.append(candle)
            elif event.kind == "positioning":
                item["long_percent"] = _optional_float(payload.get("long_percent"))
                item["short_percent"] = _optional_float(payload.get("short_percent"))
                item["open_interest"] = _optional_float(payload.get("open_interest"))
            item["updated_at_ms"] = max(item["updated_at_ms"], event.timestamp_ms)
            item["field_timestamps"][event.kind] = event.timestamp_ms
            self._revision[event.pair] += 1
            return True

    async def snapshot(self, pair: str) -> MarketSnapshot | None:
        async with self._lock:
            item = self._items.get(pair)
            if item is None:
                return None
            candles = {tf: tuple(values) for tf, values in item["candles"].items()}
            return MarketSnapshot(
                pair=pair,
                revision=self._revision[pair],
                updated_at_ms=item["updated_at_ms"],
                field_timestamps=dict(item["field_timestamps"]),
                price=item["price"],
                mark_price=item["mark_price"],
                volume_24h=item["volume_24h"],
                change_24h_pct=item["change_24h_pct"],
                funding_rate=item["funding_rate"],
                bids=dict(item["bids"]),
                asks=dict(item["asks"]),
                candles=candles,
                long_percent=item["long_percent"],
                short_percent=item["short_percent"],
                open_interest=item["open_interest"],
            )

    async def snapshots(self) -> tuple[MarketSnapshot, ...]:
        async with self._lock:
            pairs = tuple(self._items)
        values = await asyncio.gather(*(self.snapshot(pair) for pair in pairs))
        return tuple(value for value in values if value is not None)


def _first(payload: dict, *keys: str):
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def _float(value) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("market event contains a non-finite number")
    return parsed


def _optional_float(value) -> float | None:
    if value is None:
        return None
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("market event contains a non-finite number")
    return parsed


def _parse_candle(payload: dict) -> Candle | None:
    try:
        open_time = int(payload.get("open_time", payload.get("t", payload.get("time"))))
        # Futures websocket candle payloads use open_time in seconds; current
        # websocket `t` and REST `time` values are already milliseconds.
        if 0 < open_time < 10_000_000_000:
            open_time *= 1000
        values = [
            float(payload.get("open", payload.get("o"))),
            float(payload.get("high", payload.get("h"))),
            float(payload.get("low", payload.get("l"))),
            float(payload.get("close", payload.get("c"))),
            float(payload.get("volume", payload.get("v", 0)) or 0),
        ]
        if not all(math.isfinite(value) for value in values):
            return None
        open_price, high, low, close, volume = values
        if min(open_price, high, low, close) <= 0 or volume < 0 or low > high:
            return None
        return Candle(
            open_time_ms=open_time,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume,
        )
    except (TypeError, ValueError):
        return None
