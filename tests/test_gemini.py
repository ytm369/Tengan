import json
import unittest
from unittest.mock import patch

from coindcx_futures.gemini import GeminiError, GeminiReviewer, GeminiUnavailable, RESPONSE_SCHEMA
from coindcx_futures.types import Candidate


class GeminiReviewerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.candidates = (Candidate(
            pair="B-ETH_USDT", side="long", score=0.8, revision=4,
            features={"price": 100.0, "funding_rate": None}, reasons=("momentum positive",),
        ),)

    async def test_missing_key_fails_without_request(self):
        reviewer = GeminiReviewer("gemini-3.1-flash-lite", api_key="")
        with self.assertRaises(GeminiUnavailable):
            await reviewer.review(self.candidates)

    async def test_accepts_structured_matching_review(self):
        reviewer = GeminiReviewer("gemini-3.1-flash-lite", api_key="test-key")
        response = {
            "candidates": [{"content": {"parts": [{"text": json.dumps({"reviews": [{
                "pair": "B-ETH_USDT", "decision": "long", "confidence": 82,
                "stop_loss": 95, "take_profit": 110, "quantity_suggestion": 0,
                "rationale": "Trend and momentum agree.",
            }]})}]}}]
        }
        captured = {}
        def request(body):
            captured.update(body)
            return response
        reviewer._request = request
        async def inline(function, *args):
            return function(*args)
        with patch("coindcx_futures.gemini.asyncio.to_thread", inline):
            result = await reviewer.review(self.candidates)
        self.assertEqual(result[0].decision, "long")
        self.assertIsNone(result[0].quantity_suggestion)
        self.assertEqual(
            captured["generationConfig"]["responseFormat"]["text"]["schema"], RESPONSE_SCHEMA
        )

    async def test_direction_reversal_is_rejected(self):
        reviewer = GeminiReviewer("gemini-3.1-flash-lite", api_key="test-key")
        response = {
            "candidates": [{"content": {"parts": [{"text": json.dumps({"reviews": [{
                "pair": "B-ETH_USDT", "decision": "short", "confidence": 82,
                "stop_loss": 105, "take_profit": 90, "quantity_suggestion": 0,
                "rationale": "Reverse.",
            }]})}]}}]
        }
        reviewer._request = lambda _body: response
        async def inline(function, *args):
            return function(*args)
        with patch("coindcx_futures.gemini.asyncio.to_thread", inline):
            with self.assertRaises(GeminiError):
                await reviewer.review(self.candidates)


if __name__ == "__main__":
    unittest.main()
