import tempfile
import unittest
from pathlib import Path

from coindcx_futures.paper import PaperOutcomeTracker
from coindcx_futures.storage import AuditStore
from coindcx_futures.types import MarketEvent


class PaperOutcomeTests(unittest.TestCase):
    def test_tracks_and_persists_outcomes_then_restores_only_open_signals(self):
        with tempfile.TemporaryDirectory() as folder:
            store = AuditStore(Path(folder) / "audit.sqlite3")
            try:
                tracker = PaperOutcomeTracker(store)
                opened = tracker.open(
                    pair="B-ETH_USDT", side="long", entry_price=100,
                    stop_loss=95, take_profit=110, timestamp_ms=1_000,
                    confidence=80, score=0.7, rationale="test",
                )
                self.assertTrue(opened)
                self.assertFalse(tracker.open(
                    pair="B-ETH_USDT", side="long", entry_price=100,
                    stop_loss=95, take_profit=110, timestamp_ms=1_001,
                    confidence=80, score=0.7, rationale="duplicate",
                ))

                restored = PaperOutcomeTracker(store)
                self.assertIn("B-ETH_USDT", restored.open_positions)
                restored.on_market_event(
                    MarketEvent("B-ETH_USDT", "ticker", 2_000, "test", {"ls": 111}), 111
                )

                self.assertNotIn("B-ETH_USDT", restored.open_positions)
                summary = store.recent_paper_outcomes("B-ETH_USDT")
                self.assertEqual(summary["closed_trades"], 1)
                self.assertEqual(summary["win_rate"], 1.0)
                self.assertEqual(summary["take_profit_exits"], 1)
                self.assertEqual(store.load_open_paper_trades(), [])
            finally:
                store.close()

    def test_ignores_non_price_ticker_updates(self):
        with tempfile.TemporaryDirectory() as folder:
            store = AuditStore(Path(folder) / "audit.sqlite3")
            try:
                tracker = PaperOutcomeTracker(store)
                tracker.open(
                    pair="B-ETH_USDT", side="short", entry_price=100,
                    stop_loss=110, take_profit=90, timestamp_ms=1_000,
                    confidence=80, score=0.7, rationale="test",
                )
                tracker.on_market_event(
                    MarketEvent("B-ETH_USDT", "ticker", 2_000, "test", {"mp": 89}), 89
                )
                self.assertIn("B-ETH_USDT", tracker.open_positions)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
