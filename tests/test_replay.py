import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from coindcx_futures.config import Settings
from coindcx_futures.replay import replay_market_events
from coindcx_futures.storage import AuditStore
from coindcx_futures.types import MarketEvent


class ReplayTests(unittest.TestCase):
    def test_replay_ranks_candidates_without_external_services(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "audit.sqlite3"
            store = AuditStore(path)
            now = int(time.time() * 1000)
            events = []
            for pair, change in (("B-BTC_USDT", 1.0), ("B-ETH_USDT", 10.0), ("B-SOL_USDT", -10.0)):
                events.append(MarketEvent(pair, "ticker", now, "test", {
                    "ls": 100, "mp": 100, "v": 1_000_000, "pc": change, "fr": 0.00001,
                }))
                events.append(MarketEvent(pair, "orderbook", now, "test", {
                    "bids": {"99.9": "100", "99.8": "100"},
                    "asks": {"100.1": "100", "100.2": "100"},
                }))
                for index, close in enumerate((99.0, 100.0)):
                    events.append(MarketEvent(pair, "candle", now - 2_000 + index * 1_000, "test", {
                        "open_time": now - 3_000 + index * 1_000,
                        "open": close - 0.1, "high": close + 0.2,
                        "low": close - 0.2, "close": close, "volume": 1_000,
                    }, timeframe="1m"))
            try:
                for event in events:
                    store.log_event(event)
            finally:
                store.close()

            count, candidates = asyncio.run(replay_market_events(path, Settings()))
            self.assertEqual(count, len(events))
            self.assertTrue(any(item.pair == "B-ETH_USDT" and item.side == "long" for item in candidates))
            self.assertTrue(any(item.pair == "B-SOL_USDT" and item.side == "short" for item in candidates))


if __name__ == "__main__":
    unittest.main()
