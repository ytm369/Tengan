import unittest

from coindcx_futures.coindcx import _channel_pair, _channel_pair_and_tf, _unwrap
from coindcx_futures.state import _parse_candle


class CoinDCXPayloadTests(unittest.TestCase):
    def test_orderbook_symbol_resolves_to_subscribed_pair(self):
        pair = _channel_pair(
            {"data": {"s": "ETHUSDT", "bids": {"99": "1"}, "asks": {"101": "1"}}},
            {}, {"ETHUSDT": "B-ETH_USDT"},
        )
        self.assertEqual(pair, "B-ETH_USDT")

    def test_futures_candle_array_uses_channel_and_latest_candle(self):
        message = {
            "Ets": 1_700_000_001_000,
            "i": "1m",
            "channel": "B-ETH_USDT_1m-futures",
            "data": [
                {"open_time": 1_700_000_000, "open": "99", "high": "101", "low": "98", "close": "100"},
                {"open_time": 1_700_000_060, "open": "100", "high": "102", "low": "99", "close": "101"},
            ],
        }
        self.assertEqual(
            _channel_pair_and_tf(message, {"B-ETH_USDT_1m-futures": ("B-ETH_USDT", "1m")}),
            ("B-ETH_USDT", "1m"),
        )
        latest = _unwrap(message)
        candle = _parse_candle(latest)
        self.assertEqual(candle.open_time_ms, 1_700_000_060_000)
        self.assertEqual(candle.close, 101)


if __name__ == "__main__":
    unittest.main()
