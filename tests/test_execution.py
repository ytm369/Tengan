import unittest
import time

from coindcx_futures.config import Settings
from coindcx_futures.execution import LiveExecutor
from coindcx_futures.state import MarketState
from coindcx_futures.types import InstrumentSpec, MarketEvent, TradeProposal


class MemoryStore:
    def __init__(self):
        self.rows = []

    def log_trade(self, timestamp, kind, payload):
        self.rows.append((kind, payload))


class FakePrivate:
    def __init__(self):
        self.exited = []
        self.position_quantity = 1.0
        self.order_response = [{"id": "order-1", "status": "filled"}]
        self.open_orders = []
        self.cancelled = []
        self.protection_response = {
            "stop_loss": {"id": "stop-1", "status": "untriggered"},
            "take_profit": {"id": "target-1", "status": "untriggered"},
        }

    async def get_positions(self, _currency):
        return [{"id": "position-1", "pair": "B-ETH_USDT", "active_pos": self.position_quantity, "avg_price": 100}]

    async def get_conversion_rate(self):
        return 80

    async def get_today_transactions(self, _currency):
        return []

    async def create_market_order(self, _proposal, _currency):
        return self.order_response

    async def get_open_orders(self, _side, _currency):
        return list(self.open_orders)

    async def cancel_order(self, order_id):
        self.cancelled.append(order_id)
        self.open_orders = [row for row in self.open_orders if str(row.get("id")) != order_id]
        return {"code": 200, "message": "success"}

    async def create_tpsl(self, _position_id, _stop, _target):
        return self.protection_response

    async def exit_position(self, position_id):
        self.exited.append(position_id)
        return {"code": 200}


class FakePublic:
    async def get_instrument(self, _pair, _currency):
        return InstrumentSpec(
            pair="B-ETH_USDT", quantity_increment=0.1, min_quantity=0.1,
            max_quantity=100, unit_contract_value=1, max_leverage=10,
        )


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_open_exposure_uses_fresh_market_price_instead_of_position_mark(self):
        now = int(time.time() * 1000)
        state = MarketState()
        await state.apply(MarketEvent(
            "B-ETH_USDT", "ticker", now, "test", {"ls": 120, "mp": 100},
        ))
        private = FakePrivate()
        store = MemoryStore()
        executor = LiveExecutor(private, FakePublic(), state, store, Settings(max_entry_age_seconds=10))

        account, _ = await executor._account_state()

        self.assertEqual(account.total_exposure_inr, 120 * 80)

    async def test_open_position_with_stale_ticker_blocks_account_risk_check(self):
        state = MarketState()
        old = int(time.time() * 1000) - 60_000
        await state.apply(MarketEvent("B-ETH_USDT", "ticker", old, "test", {"ls": 120}))
        executor = LiveExecutor(FakePrivate(), FakePublic(), state, MemoryStore(), Settings(max_entry_age_seconds=10))

        with self.assertRaisesRegex(RuntimeError, "unavailable or stale"):
            await executor._account_state()

    async def test_failed_protection_attempts_exit_and_halts_entries(self):
        state = MarketState()
        now = 2_000_000_000_000
        await state.apply(MarketEvent("B-ETH_USDT", "ticker", now, "test", {"ls": 100}))
        private = FakePrivate()
        private.protection_response = {
            "stop_loss": {"success": False, "error": "trigger rejected"},
            "take_profit": {"id": "target-1", "status": "untriggered"},
        }
        store = MemoryStore()
        settings = Settings(max_entry_age_seconds=10)
        executor = LiveExecutor(private, FakePublic(), state, store, settings)
        result = await executor._submit_and_protect(
            TradeProposal("B-ETH_USDT", "long", 100, 95, 110, 1, 1),
            await FakePublic().get_instrument("B-ETH_USDT", "INR"),
        )
        self.assertFalse(result)
        self.assertTrue(executor.halted)
        self.assertEqual(private.exited, ["position-1"])
        self.assertIn("execution_halt", [kind for kind, _ in store.rows])

    async def test_definitive_order_rejection_does_not_halt_or_attach_protection(self):
        private = FakePrivate()
        private.order_response = [{"id": "order-1", "status": "rejected"}]
        private.position_quantity = 0
        store = MemoryStore()
        executor = LiveExecutor(private, FakePublic(), MarketState(), store, Settings())
        proposal = TradeProposal("B-ETH_USDT", "long", 100, 95, 110, 1, 1)

        result = await executor._submit_and_protect(
            proposal, await FakePublic().get_instrument("B-ETH_USDT", "INR")
        )

        self.assertFalse(result)
        self.assertFalse(executor.halted)
        self.assertFalse(private.exited)
        self.assertIn("order_rejected", [kind for kind, _ in store.rows])

    async def test_partial_fill_is_protected_at_the_confirmed_position_size(self):
        private = FakePrivate()
        private.order_response = [{"id": "order-1", "status": "partially_filled"}]
        private.position_quantity = 0.4
        private.open_orders = [{"id": "order-1", "status": "partially_filled", "remaining_quantity": 0.6}]
        store = MemoryStore()
        executor = LiveExecutor(private, FakePublic(), MarketState(), store, Settings())
        proposal = TradeProposal("B-ETH_USDT", "long", 100, 95, 110, 1, 1)

        result = await executor._submit_and_protect(
            proposal, await FakePublic().get_instrument("B-ETH_USDT", "INR")
        )

        self.assertTrue(result)
        self.assertFalse(executor.halted)
        self.assertFalse(private.exited)
        self.assertEqual(private.cancelled, ["order-1"])
        protected = next(payload for kind, payload in store.rows if kind == "protected_position")
        self.assertEqual(protected["quantity"], 0.4)
        self.assertEqual(protected["approved_quantity"], 1)


if __name__ == "__main__":
    unittest.main()
