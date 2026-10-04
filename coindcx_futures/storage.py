from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class AuditStore:
    """Small local append-only audit database; credentials are never stored here."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS market_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL, kind TEXT NOT NULL, timestamp_ms INTEGER NOT NULL,
                source TEXT NOT NULL, timeframe TEXT, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS market_events_pair_time ON market_events(pair, timestamp_ms);
            CREATE TABLE IF NOT EXISTS node_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT, node TEXT NOT NULL, pair TEXT NOT NULL,
                input_revision INTEGER NOT NULL, timestamp_ms INTEGER NOT NULL, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS model_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp_ms INTEGER NOT NULL, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trade_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp_ms INTEGER NOT NULL,
                kind TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS trade_records_kind_id ON trade_records(kind, id);
            CREATE TABLE IF NOT EXISTS paper_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, pair TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL, side TEXT NOT NULL,
                return_pct REAL NOT NULL, exit_reason TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS paper_outcomes_pair_time ON paper_outcomes(pair, timestamp_ms);
            CREATE TABLE IF NOT EXISTS config_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp_ms INTEGER NOT NULL, version INTEGER NOT NULL,
                payload TEXT NOT NULL
            );
        """)
        self.connection.commit()

    def log_event(self, event) -> None:
        self.connection.execute(
            "INSERT INTO market_events(pair,kind,timestamp_ms,source,timeframe,payload) VALUES(?,?,?,?,?,?)",
            (event.pair, event.kind, event.timestamp_ms, event.source, event.timeframe, _json(event.payload)),
        )
        self.connection.commit()

    def log_node_result(self, result) -> None:
        self.connection.execute(
            "INSERT INTO node_results(node,pair,input_revision,timestamp_ms,payload) VALUES(?,?,?,?,?)",
            (result.node, result.pair, result.input_revision, result.timestamp_ms, _json(result)),
        )
        self.connection.commit()

    def log_model_review(self, timestamp_ms: int, payload: Any) -> None:
        self.connection.execute("INSERT INTO model_reviews(timestamp_ms,payload) VALUES(?,?)", (timestamp_ms, _json(payload)))
        self.connection.commit()

    def log_trade(self, timestamp_ms: int, kind: str, payload: Any) -> None:
        self.connection.execute("INSERT INTO trade_records(timestamp_ms,kind,payload) VALUES(?,?,?)", (timestamp_ms, kind, _json(payload)))
        self.connection.commit()

    def log_paper_outcome(self, payload: dict[str, Any]) -> None:
        timestamp_ms = int(payload["closed_at_ms"])
        encoded = _json(payload)
        self.connection.execute(
            "INSERT INTO paper_outcomes(pair,timestamp_ms,side,return_pct,exit_reason,payload) VALUES(?,?,?,?,?,?)",
            (payload["pair"], timestamp_ms, payload["side"], float(payload["return_pct"]), payload["exit_reason"], encoded),
        )
        self.connection.execute(
            "INSERT INTO trade_records(timestamp_ms,kind,payload) VALUES(?,?,?)",
            (timestamp_ms, "paper_outcome", encoded),
        )
        self.connection.commit()

    def load_open_paper_trades(self) -> list[dict[str, Any]]:
        opened: dict[str, dict[str, Any]] = {}
        rows = self.connection.execute(
            "SELECT kind,payload FROM trade_records WHERE kind IN ('paper_signal','paper_outcome') ORDER BY id"
        )
        for kind, encoded in rows:
            try:
                payload = json.loads(encoded)
                pair = str(payload["pair"])
            except (KeyError, TypeError, json.JSONDecodeError):
                continue
            if kind == "paper_signal":
                opened[pair] = payload
            else:
                opened.pop(pair, None)
        return list(opened.values())

    def recent_paper_outcomes(self, pair: str, limit: int = 20) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT return_pct,exit_reason FROM paper_outcomes WHERE pair=? ORDER BY id DESC LIMIT ?",
            (pair, max(1, int(limit))),
        ).fetchall()
        returns = [float(row[0]) for row in rows]
        wins = sum(value > 0 for value in returns)
        return {
            "closed_trades": len(returns),
            "win_rate": round(wins / len(returns), 3) if returns else None,
            "mean_return_pct": round(sum(returns) / len(returns), 4) if returns else None,
            "stop_loss_exits": sum(row[1] == "stop_loss" for row in rows),
            "take_profit_exits": sum(row[1] == "take_profit" for row in rows),
            "sample_size": len(returns),
        }

    def log_config(self, timestamp_ms: int, version: int, payload: Any) -> None:
        self.connection.execute("INSERT INTO config_changes(timestamp_ms,version,payload) VALUES(?,?,?)", (timestamp_ms, version, _json(payload)))
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def _json(value: Any) -> str:
    if hasattr(value, "__dataclass_fields__"):
        from dataclasses import asdict
        value = asdict(value)
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
