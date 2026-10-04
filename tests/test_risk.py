import unittest

from coindcx_futures.config import RiskLimits
from coindcx_futures.risk import AccountRiskState, RiskEngine
from coindcx_futures.types import InstrumentSpec, TradeProposal


class RiskEngineTests(unittest.TestCase):
    def setUp(self):
        self.engine = RiskEngine()
        self.limits = RiskLimits(1000, 3000, 50000, 3, 4)
        self.spec = InstrumentSpec(
            pair="B-ETH_USDT", price_increment=0.01, quantity_increment=0.1,
            min_quantity=0.1, max_quantity=100, unit_contract_value=1, max_leverage=10,
        )
        self.proposal = TradeProposal("B-ETH_USDT", "long", 100, 95, 110, 0, 8)
        self.account = AccountRiskState(usdt_inr_conversion=80)

    def assess(self, **overrides):
        options = dict(
            live_armed=True, model_confidence=90, minimum_model_confidence=65,
        )
        options.update(overrides)
        return self.engine.assess(self.proposal, self.spec, self.limits, self.account, **options)

    def test_refuses_unarmed_live_orders(self):
        decision = self.assess(live_armed=False)
        self.assertFalse(decision.approved)
        self.assertIn("not armed", decision.reason)

    def test_calculates_quantity_from_trade_risk_and_exposure_caps(self):
        decision = self.assess()
        self.assertTrue(decision.approved, decision.reason)
        self.assertEqual(decision.quantity, 2.5)
        self.assertEqual(decision.leverage, 4)

    def test_refuses_daily_loss_and_duplicate_pair(self):
        loss = AccountRiskState(realized_pnl_today_inr=-3000, usdt_inr_conversion=80)
        duplicate = AccountRiskState(open_pairs=frozenset({"B-ETH_USDT"}), usdt_inr_conversion=80)
        self.assertIn("daily loss", self.engine.assess(
            self.proposal, self.spec, self.limits, loss, live_armed=True,
            model_confidence=90, minimum_model_confidence=65,
        ).reason)
        self.assertIn("already exists", self.engine.assess(
            self.proposal, self.spec, self.limits, duplicate, live_armed=True,
            model_confidence=90, minimum_model_confidence=65,
        ).reason)

    def test_refuses_missing_exchange_quantity_metadata(self):
        incomplete = InstrumentSpec(pair="B-ETH_USDT", unit_contract_value=1)
        decision = self.engine.assess(
            self.proposal, incomplete, self.limits, self.account, live_armed=True,
            model_confidence=90, minimum_model_confidence=65,
        )
        self.assertFalse(decision.approved)
        self.assertIn("increments", decision.reason)


if __name__ == "__main__":
    unittest.main()

