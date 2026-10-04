import asyncio
import time
import unittest

from coindcx_futures.analysis import AnalysisPipeline
from coindcx_futures.bus import EventBus
from coindcx_futures.config import Settings
from coindcx_futures.state import MarketState
from coindcx_futures.types import MarketEvent


class AnalysisPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_independent_nodes_process_shared_events(self):
        state = MarketState()
        bus = EventBus(queue_size=128)
        pipeline = AnalysisPipeline(state, bus, Settings())
        task = asyncio.create_task(pipeline.run())
        try:
            await asyncio.sleep(0)
            now = int(time.time() * 1000)
            events = []
            for pair, change in (("B-BTC_USDT", 1.0), ("B-ETH_USDT", 5.0), ("B-SOL_USDT", -3.0)):
                events.append(MarketEvent(pair, "ticker", now, "test", {
                    "ls": 100, "mp": 100, "v": 1000000, "pc": change, "fr": 0.00005,
                }))
                events.append(MarketEvent(pair, "orderbook", now, "test", {
                    "bids": {"99.9": "100", "99.8": "100"},
                    "asks": {"100.1": "100", "100.2": "100"},
                }))
                for index, close in enumerate((99.0, 100.0)):
                    events.append(MarketEvent(pair, "candle", now - 2000 + index * 1000, "test", {
                        "open_time": now - 3000 + index * 1000,
                        "open": close - 0.1, "high": close + 0.2, "low": close - 0.2,
                        "close": close, "volume": 1000,
                    }, timeframe="1m"))
            for event in events:
                await state.apply(event)
            for event in events:
                bus.publish(event)
            for _ in range(50):
                if all(pipeline.results.get(name, {}).get("B-ETH_USDT") for name in (
                    "liquidity", "momentum", "trend", "relative_strength", "funding_positioning",
                )) and pipeline.global_results.get("btc_regime"):
                    break
                await asyncio.sleep(0.01)
            self.assertIn("B-ETH_USDT", pipeline.results["liquidity"])
            self.assertIn("B-ETH_USDT", pipeline.results["momentum"])
            self.assertIn("B-ETH_USDT", pipeline.results["trend"])
            self.assertIn("B-ETH_USDT", pipeline.results["relative_strength"])
            self.assertIn("B-ETH_USDT", pipeline.results["funding_positioning"])
            self.assertIn("btc_regime", pipeline.global_results)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()

