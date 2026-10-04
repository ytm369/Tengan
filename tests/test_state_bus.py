import asyncio
import unittest

from coindcx_futures.bus import EventBus
from coindcx_futures.state import MarketState
from coindcx_futures.types import MarketEvent


class EventBusTests(unittest.TestCase):
    def test_fanout_is_independent_and_keeps_latest_when_subscriber_is_slow(self):
        bus = EventBus(queue_size=1)
        first = bus.subscribe()
        second = bus.subscribe()
        bus.publish("old")
        bus.publish("new")
        self.assertEqual(first.queue.get_nowait(), "new")
        self.assertEqual(second.queue.get_nowait(), "new")
        self.assertEqual(first.dropped, 1)
        self.assertEqual(second.dropped, 1)


class MarketStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_out_of_order_price_but_accepts_same_candle_bucket_updates(self):
        state = MarketState()
        self.assertTrue(await state.apply(MarketEvent(
            "B-ETH_USDT", "ticker", 2000, "test", {"ls": 10, "cmRT": 2000},  source_sequence=2,
        )))
        self.assertFalse(await state.apply(MarketEvent(
            "B-ETH_USDT", "ticker", 1000, "test", {"ls": 9}, source_sequence=3,
        )))
        self.assertTrue(await state.apply(MarketEvent(
            "B-ETH_USDT", "candle", 3000, "test",
            {"open_time": 3000, "open": 10, "high": 11, "low": 9, "close": 10}, "1m",
        )))
        self.assertTrue(await state.apply(MarketEvent(
            "B-ETH_USDT", "candle", 3000, "test",
            {"open_time": 3000, "open": 10, "high": 12, "low": 9, "close": 11}, "1m",
        )))
        snapshot = await state.snapshot("B-ETH_USDT")
        self.assertEqual(snapshot.price, 10)
        self.assertEqual(len(snapshot.candles["1m"]), 1)
        self.assertEqual(snapshot.candles["1m"][0].close, 11)

    async def test_tracks_field_freshness_independently(self):
        state = MarketState()
        await state.apply(MarketEvent("B-BTC_USDT", "ticker", 1000, "test", {"ls": 10, "fr": 0.0001}))
        await state.apply(MarketEvent("B-BTC_USDT", "orderbook", 2000, "test", {"bids": {}, "asks": {}}))
        snapshot = await state.snapshot("B-BTC_USDT")
        self.assertEqual(snapshot.field_timestamps["ticker"], 1000)
        self.assertEqual(snapshot.field_timestamps["orderbook"], 2000)
        self.assertEqual(snapshot.field_timestamps["funding_rate"], 1000)


if __name__ == "__main__":
    unittest.main()

