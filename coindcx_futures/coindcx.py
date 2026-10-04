from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable
from typing import Any

from .types import InstrumentSpec, MarketEvent

EventSink = Callable[[MarketEvent], Awaitable[None]]
LOG = logging.getLogger(__name__)


class CoinDCXError(RuntimeError):
    pass


class CoinDCXPublicClient:
    API_BASE = "https://api.coindcx.com"
    PUBLIC_BASE = "https://public.coindcx.com"

    def __init__(self, timeout: float = 10.0) -> None:
        self.timeout = timeout

    async def get_active_instruments(self, margin_currency: str = "INR") -> list[str]:
        query = urllib.parse.urlencode({"margin_currency_short_name[]": margin_currency})
        data = await self._request_json("GET", f"{self.API_BASE}/exchange/v1/derivatives/futures/data/active_instruments?{query}")
        if not isinstance(data, list):
            raise CoinDCXError("CoinDCX returned an invalid active-instruments response")
        return [str(pair) for pair in data if str(pair).startswith("B-")]

    async def get_current_prices(self) -> dict[str, dict[str, Any]]:
        data = await self._request_json("GET", f"{self.PUBLIC_BASE}/market_data/v3/current_prices/futures/rt")
        prices = data.get("prices", {}) if isinstance(data, dict) else {}
        return prices if isinstance(prices, dict) else {}

    async def get_instrument(self, pair: str, margin_currency: str = "INR") -> InstrumentSpec:
        query = urllib.parse.urlencode({"pair": pair, "margin_currency_short_name": margin_currency})
        data = await self._request_json("GET", f"{self.API_BASE}/exchange/v1/derivatives/futures/data/instrument?{query}")
        raw = data.get("instrument", {}) if isinstance(data, dict) else {}
        if not raw or str(raw.get("status", "active")).lower() != "active":
            raise CoinDCXError(f"CoinDCX instrument {pair} is unavailable or inactive")
        max_quantity = _number(raw.get("max_quantity"))
        max_market_quantity = _number(raw.get("max_market_order_quantity"))
        if max_market_quantity > 0:
            max_quantity = min(max_quantity, max_market_quantity) if max_quantity > 0 else max_market_quantity
        return InstrumentSpec(
            pair=pair,
            margin_currency=margin_currency,
            quote_currency=str(raw.get("quote_currency_short_name", "USDT")),
            price_increment=_number(raw.get("price_increment")),
            quantity_increment=_number(raw.get("quantity_increment")),
            min_quantity=_number(raw.get("min_quantity")),
            max_quantity=max_quantity,
            min_notional=_number(raw.get("min_notional")),
            unit_contract_value=_number(raw.get("unit_contract_value"), 1.0),
            max_leverage=_number(raw.get("max_leverage_long"), 1.0),
        )

    async def get_orderbook(self, pair: str, depth: int = 50) -> dict[str, Any]:
        if depth not in {10, 20, 50}:
            raise ValueError("CoinDCX futures orderbook depth must be 10, 20, or 50")
        url = f"{self.PUBLIC_BASE}/market_data/v3/orderbook/{urllib.parse.quote(pair, safe='-')}-futures/{depth}"
        return await self._request_json("GET", url)

    async def get_candles(self, pair: str, resolution: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        from_seconds, to_seconds = int(start_ms / 1000), int(end_ms / 1000)
        query = urllib.parse.urlencode({
            "pair": pair, "from": from_seconds, "to": to_seconds,
            "resolution": resolution, "pcode": "f",
        })
        data = await self._request_json("GET", f"{self.PUBLIC_BASE}/market_data/candlesticks?{query}")
        if isinstance(data, dict):
            data = data.get("data", [])
        if not isinstance(data, list):
            return []
        return [row for row in data if isinstance(row, dict)]

    async def _request_json(self, method: str, url: str, body: dict | None = None, headers: dict | None = None) -> Any:
        encoded = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        request_headers = {
            "Accept": "application/json",
            "User-Agent": "coindcx-futures-framework/0.1",
        }
        request_headers.update(headers or {})
        request = urllib.request.Request(url, data=encoded, headers=request_headers, method=method)
        return await asyncio.to_thread(_read_json, request, self.timeout)


class CoinDCXPrivateClient:
    """Signed CoinDCX futures requests; instantiate only for explicitly armed live mode."""

    API_BASE = "https://api.coindcx.com"

    def __init__(self, api_key: str, api_secret: str, timeout: float = 10.0) -> None:
        if not api_key or not api_secret:
            raise ValueError("CoinDCX API key and secret are required for live execution")
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.timeout = timeout

    async def create_market_order(self, proposal, margin_currency: str = "INR") -> dict[str, Any]:
        body = {
            "timestamp": int(time.time() * 1000),
            "order": {
                "side": "buy" if proposal.side == "long" else "sell",
                "pair": proposal.pair,
                "order_type": "market_order",
                "total_quantity": proposal.quantity,
                "leverage": proposal.leverage,
                "notification": "no_notification",
                "hidden": False,
                "post_only": False,
                "margin_currency_short_name": [margin_currency],
            },
        }
        return await self._signed("POST", "/exchange/v1/derivatives/futures/orders/create", body)

    async def get_positions(self, margin_currency: str = "INR") -> list[dict[str, Any]]:
        body = {
            "timestamp": int(time.time() * 1000), "page": "1", "size": "500",
            "margin_currency_short_name": [margin_currency],
        }
        data = await self._signed("POST", "/exchange/v1/derivatives/futures/positions", body)
        return data if isinstance(data, list) else []

    async def get_open_orders(self, side: str, margin_currency: str = "INR") -> list[dict[str, Any]]:
        if side not in {"buy", "sell"}:
            raise ValueError("Futures order side must be buy or sell")
        body = {
            "timestamp": int(time.time() * 1000), "status": "open,partially_filled",
            "side": side, "page": "1", "size": "500",
            "margin_currency_short_name": [margin_currency],
        }
        data = await self._signed("POST", "/exchange/v1/derivatives/futures/orders", body)
        return data if isinstance(data, list) else []

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        return await self._signed("POST", "/exchange/v1/derivatives/futures/orders/cancel", {
            "timestamp": int(time.time() * 1000), "id": order_id,
        })

    async def get_conversion_rate(self) -> float:
        body = {"timestamp": int(time.time() * 1000)}
        data = await self._signed("POST", "/api/v1/derivatives/futures/data/conversions", body)
        for item in data if isinstance(data, list) else []:
            if item.get("margin_currency_short_name") == "INR" and item.get("target_currency_short_name") == "USDT":
                return _number(item.get("conversion_price"))
        raise CoinDCXError("CoinDCX did not return the INR/USDT futures conversion rate")

    async def get_today_transactions(self, margin_currency: str = "INR") -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        page = 1
        while page <= 20:
            body = {
                "timestamp": int(time.time() * 1000), "stage": "all",
                "page": str(page), "size": "500",
                "margin_currency_short_name": [margin_currency],
            }
            data = await self._signed("POST", "/exchange/v1/derivatives/futures/positions/transactions", body)
            if not isinstance(data, list) or not data:
                break
            all_rows.extend(data)
            if len(data) < 500:
                break
            if page == 20:
                raise CoinDCXError("Daily transaction history exceeded the safety pagination limit")
            page += 1
        return all_rows

    async def create_tpsl(self, position_id: str, stop_loss: float, take_profit: float) -> dict[str, Any]:
        body = {
            "timestamp": int(time.time() * 1000),
            "id": position_id,
            "take_profit": {"stop_price": str(take_profit), "order_type": "take_profit_market"},
            "stop_loss": {"stop_price": str(stop_loss), "order_type": "stop_market"},
        }
        return await self._signed("POST", "/exchange/v1/derivatives/futures/positions/create_tpsl", body)

    async def exit_position(self, position_id: str) -> dict[str, Any]:
        return await self._signed("POST", "/exchange/v1/derivatives/futures/positions/exit", {
            "timestamp": int(time.time() * 1000), "id": position_id,
        })

    async def _signed(self, method: str, path: str, body: dict) -> Any:
        encoded = json.dumps(body, separators=(",", ":")).encode()
        signature = hmac.new(self.api_secret, encoded, hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "coindcx-futures-framework/0.1",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }
        request = urllib.request.Request(self.API_BASE + path, data=encoded, headers=headers, method=method)
        return await asyncio.to_thread(_read_json, request, self.timeout)


class CoinDCXSocketFeed:
    """Public CoinDCX Socket.IO futures feed with async sink callbacks."""

    ENDPOINT = "https://stream.coindcx.com"

    def __init__(self, sink: EventSink, pairs: set[str], timeframes: tuple[str, ...]) -> None:
        self.sink = sink
        self.pairs = set(pairs)
        self._pairs_by_symbol = {_symbol_key(pair): pair for pair in self.pairs}
        self.timeframes = timeframes
        self.client = None
        self._channels: dict[str, tuple[str, str | None]] = {}
        self._ping_task: asyncio.Task | None = None
        self._closing = False

    async def run(self) -> None:
        try:
            import socketio
        except ImportError as exc:
            raise RuntimeError("Install project dependencies with: pip install -e .") from exc
        self.client = socketio.AsyncClient(reconnection=True, logger=False, engineio_logger=False)
        self._register_handlers()
        backoff = 1.0
        while not self._closing:
            try:
                if not self.client.connected:
                    await self.client.connect(self.ENDPOINT, transports=["websocket"], wait_timeout=15)
                if not self._ping_task or self._ping_task.done():
                    self._ping_task = asyncio.create_task(self._ping_loop(), name="coindcx-socket-ping")
                await self.client.wait()
                backoff = 1.0
                if not self._closing:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("CoinDCX Socket.IO connection failed; retrying: %s", exc)
                if self.client.connected:
                    await self.client.disconnect()
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2)

    async def close(self) -> None:
        self._closing = True
        if self._ping_task:
            self._ping_task.cancel()
            await asyncio.gather(self._ping_task, return_exceptions=True)
        if self.client and self.client.connected:
            await self.client.disconnect()

    async def set_pairs(self, pairs: set[str]) -> None:
        new_pairs = set(pairs)
        removed = self.pairs - new_pairs
        added = new_pairs - self.pairs
        self.pairs = new_pairs
        self._pairs_by_symbol = {_symbol_key(pair): pair for pair in new_pairs}
        if not self.client or not self.client.connected:
            return
        for pair in removed:
            channels = [f"{pair}@orderbook@50-futures", *(f"{pair}_{tf}-futures" for tf in self.timeframes)]
            for channel in channels:
                self._channels.pop(channel, None)
                await self.client.emit("leave", {"channelName": channel})
        for pair in sorted(added):
            await self._subscribe_pair(pair)

    def _register_handlers(self) -> None:
        assert self.client is not None

        @self.client.event
        async def connect():
            await self.client.emit("join", {"channelName": "currentPrices@futures@rt"})
            for pair in sorted(self.pairs):
                await self._subscribe_pair(pair)

        @self.client.on("currentPrices@futures#update")
        async def current_prices(message):
            message = _unwrap(message)
            timestamp = int(message.get("ts", time.time() * 1000))
            for pair, value in (message.get("prices") or {}).items():
                if pair not in self.pairs or not isinstance(value, dict):
                    continue
                await self.sink(MarketEvent(
                    pair=pair, kind="ticker", timestamp_ms=int(value.get("ctRT", value.get("cmRT", timestamp))),
                    source="coindcx_socket", source_sequence=_optional_int(message.get("vs")),
                    payload=dict(value),
                ))

        @self.client.on("depth-snapshot")
        async def depth_snapshot(message):
            pair = _channel_pair(message, self._channels, self._pairs_by_symbol)
            if not pair:
                return
            data = _unwrap(message)
            await self.sink(MarketEvent(
                pair=pair, kind="orderbook", timestamp_ms=int(data.get("ts", time.time() * 1000)),
                source="coindcx_socket", source_sequence=_optional_int(data.get("vs")), payload=data,
            ))

        @self.client.on("candlestick")
        async def candlestick(message):
            pair, timeframe = _channel_pair_and_tf(message, self._channels, self._pairs_by_symbol)
            if not pair or not timeframe:
                return
            data = _unwrap(message)
            outer = message if isinstance(message, dict) else {}
            timestamp = int(outer.get("Ets", data.get("eT", data.get("T", data.get("t", time.time() * 1000)))))
            await self.sink(MarketEvent(
                pair=pair, kind="candle", timeframe=timeframe, timestamp_ms=timestamp,
                source="coindcx_socket", source_sequence=_optional_int(data.get("vs")), payload=data,
            ))

    async def _ping_loop(self) -> None:
        while self.client and not self._closing:
            await asyncio.sleep(25)
            if not self.client.connected:
                continue
            try:
                await self.client.emit("ping", {"data": "Ping message"})
            except Exception:
                LOG.debug("CoinDCX Socket.IO ping failed")

    async def _subscribe_pair(self, pair: str) -> None:
        assert self.client is not None
        book_channel = f"{pair}@orderbook@50-futures"
        self._channels[book_channel] = (pair, None)
        await self.client.emit("join", {"channelName": book_channel})
        for timeframe in self.timeframes:
            candle_channel = f"{pair}_{timeframe}-futures"
            self._channels[candle_channel] = (pair, timeframe)
            await self.client.emit("join", {"channelName": candle_channel})


def _read_json(request: urllib.request.Request, timeout: float) -> Any:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read(512).decode("utf-8", errors="replace").strip()
        except Exception:
            detail = ""
        description = f"HTTP {exc.code} {exc.reason} for {request.full_url}"
        if detail:
            description += f": {detail}"
        raise CoinDCXError(description) from exc
    except Exception as exc:
        raise CoinDCXError(f"CoinDCX request failed for {request.full_url}: {exc}") from exc


def _number(value, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _optional_int(value) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _unwrap(message: Any) -> dict[str, Any]:
    if not isinstance(message, dict):
        return {}
    data = message.get("data", message)
    if isinstance(data, list):
        data = next((row for row in reversed(data) if isinstance(row, dict)), {})
    return data if isinstance(data, dict) else {}


def _channel_pair(message: Any, channels: dict[str, tuple[str, str | None]],
                  pairs_by_symbol: dict[str, str] | None = None) -> str | None:
    if isinstance(message, dict):
        channel = message.get("channel") or message.get("channelName")
        if channel in channels:
            return channels[channel][0]
        payload = _unwrap(message)
        channel = payload.get("channel") or payload.get("channelName")
        if channel in channels:
            return channels[channel][0]
        pair = payload.get("pair")
        if pair in (pairs_by_symbol or {}).values():
            return str(pair)
        symbol = payload.get("s") or payload.get("symbol")
        if symbol:
            return (pairs_by_symbol or {}).get(_symbol_key(str(symbol)))
    return None


def _channel_pair_and_tf(message: Any, channels: dict[str, tuple[str, str | None]],
                         pairs_by_symbol: dict[str, str] | None = None) -> tuple[str | None, str | None]:
    if isinstance(message, dict):
        channel = message.get("channel") or message.get("channelName")
        if channel in channels:
            return channels[channel]
        payload = _unwrap(message)
        channel = payload.get("channel") or payload.get("channelName")
        if channel in channels:
            return channels[channel]
        pair = payload.get("pair")
        timeframe = payload.get("i") or payload.get("interval")
        if not pair:
            symbol = payload.get("s") or payload.get("symbol")
            if symbol:
                pair = (pairs_by_symbol or {}).get(_symbol_key(str(symbol)))
        if pair and timeframe:
            return str(pair), str(timeframe)
    return None, None


def _symbol_key(value: str) -> str:
    """Normalize B-/G- exchange symbols such as B-ETH_USDT and ETHUSDT."""
    value = value.upper().strip()
    if "-" in value:
        prefix, remainder = value.split("-", 1)
        if prefix.isalpha():
            value = remainder
    return value.replace("_", "").replace("-", "")
