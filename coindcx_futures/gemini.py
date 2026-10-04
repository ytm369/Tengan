from __future__ import annotations

import asyncio
import json
import math
import os
import urllib.error
import urllib.request
from dataclasses import asdict

from .types import Candidate, CandidateReview


class GeminiError(RuntimeError):
    pass


class GeminiUnavailable(GeminiError):
    pass


RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "reviews": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "pair": {"type": "string"},
                    "decision": {"type": "string", "enum": ["long", "short", "skip"]},
                    "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                    "stop_loss": {"type": "number", "minimum": 0},
                    "take_profit": {"type": "number", "minimum": 0},
                    "quantity_suggestion": {"type": "number", "minimum": 0},
                    "rationale": {"type": "string"},
                },
                "required": ["pair", "decision", "confidence", "stop_loss", "take_profit", "quantity_suggestion", "rationale"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["reviews"],
    "additionalProperties": False,
}


class GeminiReviewer:
    """Direct Google Gemini Developer API client; no CLI OAuth or paid fallback."""

    API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    def __init__(self, model: str, api_key: str | None = None, timeout: float = 20.0) -> None:
        self.model = model
        self.api_key = api_key or os.getenv("GEMINI_API_KEY", "")
        self.timeout = timeout

    async def review(self, candidates: tuple[Candidate, ...]) -> tuple[CandidateReview, ...]:
        if not candidates:
            return ()
        if not self.api_key:
            raise GeminiUnavailable("GEMINI_API_KEY is not set; no model review was made")
        prompt = self._prompt(candidates)
        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "responseFormat": {
                    "text": {"mimeType": "application/json", "schema": RESPONSE_SCHEMA},
                },
            },
        }
        response = await asyncio.to_thread(self._request, body)
        try:
            text = response["candidates"][0]["content"]["parts"][0]["text"]
            decoded = json.loads(text)
            reviews = decoded["reviews"]
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise GeminiError("Gemini returned no valid structured review") from exc
        expected = {candidate.pair: candidate.side for candidate in candidates}
        parsed: dict[str, CandidateReview] = {}
        for raw in reviews:
            review = _parse_review(raw)
            if review.pair not in expected:
                raise GeminiError(f"Gemini returned an unrequested instrument: {review.pair}")
            if review.pair in parsed:
                raise GeminiError(f"Gemini returned a duplicate review for {review.pair}")
            if review.decision not in {"long", "short", "skip"}:
                raise GeminiError(f"Gemini returned an unsupported decision for {review.pair}")
            if review.decision != "skip" and review.decision != expected[review.pair]:
                raise GeminiError(f"Gemini reversed the locally ranked direction for {review.pair}")
            parsed[review.pair] = review
        if set(parsed) != set(expected):
            raise GeminiError("Gemini review did not include every submitted candidate")
        return tuple(parsed[candidate.pair] for candidate in candidates)

    def _prompt(self, candidates: tuple[Candidate, ...]) -> str:
        public_rows = [
            {
                "pair": c.pair,
                "scanner_side": c.side,
                "scanner_score": round(c.score, 4),
                "features": c.features,
                "scanner_reasons": c.reasons,
            }
            for c in candidates
        ]
        return (
            "Review these public CoinDCX market snapshots. They contain no account balances, "
            "positions, keys, or personal data. Return one review for every pair. Keep the scanner "
            "direction or choose skip; never reverse the direction. Only propose stop and target "
            "prices that are positive and correctly ordered around the current price. Use skip if "
            "data is insufficient. For skip, output 0 for stop_loss, take_profit and quantity_suggestion. "
            "Recent paper outcomes are historical scanner feedback only, not a risk limit. "
            "For a trade, quantity_suggestion may be 0 to leave sizing to the local risk engine. "
            "Quantity is only a suggestion and will be capped by local risk rules.\n\n"
            + json.dumps(public_rows, separators=(",", ":"), allow_nan=False)
        )

    def _request(self, body: dict) -> dict:
        url = self.API_URL.format(model=self.model)
        request = urllib.request.Request(
            url,
            data=json.dumps(body, separators=(",", ":")).encode(),
            headers={"Content-Type": "application/json", "x-goog-api-key": self.api_key},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:400]
            if exc.code == 429:
                raise GeminiUnavailable("Gemini free-tier quota or rate limit exhausted") from exc
            raise GeminiError(f"Gemini API returned HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise GeminiUnavailable(f"Gemini API request failed: {exc}") from exc


def _parse_review(raw: dict) -> CandidateReview:
    try:
        pair = str(raw["pair"])
        decision = str(raw["decision"]).lower()
        confidence = int(raw["confidence"])
        stop = _optional_number(raw.get("stop_loss"))
        target = _optional_number(raw.get("take_profit"))
        quantity = _optional_number(raw.get("quantity_suggestion"))
        rationale = str(raw["rationale"])
    except (KeyError, TypeError, ValueError) as exc:
        raise GeminiError("Gemini review has invalid fields") from exc
    if not 0 <= confidence <= 100:
        raise GeminiError(f"Gemini confidence outside 0..100 for {pair}")
    if decision != "skip" and (stop is None or target is None):
        raise GeminiError(f"Gemini did not provide protection levels for {pair}")
    if quantity is not None and quantity <= 0:
        raise GeminiError(f"Gemini suggested a nonpositive quantity for {pair}")
    return CandidateReview(pair, decision, confidence, stop, target, quantity, rationale)


def _optional_number(value) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError("Expected a nonnegative finite number")
    if result == 0:
        return None
    return result
