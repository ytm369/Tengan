from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any


def run_monitor(db_path: Path, view: str, stale_after_seconds: float = 20.0) -> None:
    """Render a read-only, polling terminal view of a running scanner."""
    if view not in {"all", "market", "analysis", "leaderboard", "model", "trades"}:
        raise ValueError(f"unsupported monitor view: {view}")
    try:
        while True:
            try:
                sections = _read_view(db_path, view, stale_after_seconds)
            except (sqlite3.Error, OSError) as exc:
                sections = [("DATABASE", [f"Waiting for {db_path}: {exc}"])]
            _render(view, db_path, sections)
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")


def _read_view(db_path: Path, view: str, stale_after_seconds: float) -> list[tuple[str, list[str]]]:
    if not db_path.exists():
        return [("DATABASE", [f"No database yet: {db_path}", "Start the scanner in another terminal first."])]
    connection = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=1.0)
    connection.row_factory = sqlite3.Row
    try:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "market_events" not in tables:
            return [("DATABASE", ["Audit database exists, but scanner tables have not been created yet."])]
        sections: list[tuple[str, list[str]]] = []
        if view in {"all", "market"}:
            sections.append(("MARKET DATA HEALTH", _market_lines(connection, stale_after_seconds)))
        if view in {"all", "analysis"}:
            sections.append(("ANALYSIS NODES", _analysis_lines(connection, stale_after_seconds)))
        if view in {"all", "leaderboard"}:
            sections.append(("OPPORTUNITY LEADERBOARD", _leaderboard_lines(connection)))
        if view in {"all", "model"}:
            sections.append(("GEMINI REVIEWS", _model_lines(connection)))
        if view in {"all", "trades"}:
            sections.append(("TRADE / PAPER OUTCOMES", _trade_lines(connection)))
        return sections
    finally:
        connection.close()


def _market_lines(connection: sqlite3.Connection, stale_after_seconds: float) -> list[str]:
    rows = connection.execute("""
        SELECT e.pair, e.kind, e.timestamp_ms, e.source
        FROM market_events e
        JOIN (SELECT pair, kind, MAX(timestamp_ms) AS latest FROM market_events GROUP BY pair, kind) latest
          ON latest.pair=e.pair AND latest.kind=e.kind AND latest.latest=e.timestamp_ms
        ORDER BY e.pair, e.kind
    """).fetchall()
    if not rows:
        return ["No market events recorded yet."]
    now_ms = int(time.time() * 1000)
    latest: dict[str, dict[str, sqlite3.Row]] = {}
    for row in rows:
        latest.setdefault(row["pair"], {})[row["kind"]] = row
    fresh_tickers = sum(
        "ticker" in kinds and now_ms - int(kinds["ticker"]["timestamp_ms"]) <= stale_after_seconds * 1000
        for kinds in latest.values()
    )
    lines = [f"Fresh tickers: {fresh_tickers}/{len(latest)}  |  stale after {stale_after_seconds:g}s"]
    lines.append("PAIR             TICKER AGE/SOURCE                  CANDLE AGE/SOURCE                  BOOK AGE/SOURCE")
    for pair, kinds in sorted(latest.items()):
        values = []
        for kind in ("ticker", "candle", "orderbook"):
            event = kinds.get(kind)
            if event is None:
                values.append("missing")
                continue
            age = max(0.0, (now_ms - int(event["timestamp_ms"])) / 1000)
            freshness = "FRESH" if kind != "ticker" or age <= stale_after_seconds else "STALE"
            values.append(f"{age:5.0f}s {freshness} {event['source']}")
        lines.append(f"{pair:<16} {values[0]:<31} {values[1]:<31} {values[2]}")
    if fresh_tickers == 0:
        lines.append("ALERT: No fresh tickers. Candidate ranking cannot run on current prices.")
    return lines


def _analysis_lines(connection: sqlite3.Connection, stale_after_seconds: float) -> list[str]:
    try:
        rows = connection.execute("""
            SELECT node, pair, input_revision, timestamp_ms, payload
            FROM node_results n
            WHERE id=(SELECT MAX(id) FROM node_results latest WHERE latest.node=n.node AND latest.pair=n.pair)
            ORDER BY node, pair
        """).fetchall()
    except sqlite3.Error:
        return ["No node results table yet."]
    if not rows:
        return ["No analysis-node results yet."]
    now_ms = int(time.time() * 1000)
    by_node: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        try:
            payload = json.loads(row["payload"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        by_node.setdefault(row["node"], []).append({
            "pair": row["pair"], "timestamp": int(row["timestamp_ms"]),
            "score": payload.get("score"), "available": payload.get("available", True),
            "features": payload.get("features", {}),
        })
    lines = ["NODE                 FRESH/RESULTS   RECENT SCORES (pair: score | reason)"]
    for node, values in sorted(by_node.items()):
        fresh = [v for v in values if now_ms - v["timestamp"] <= stale_after_seconds * 1000]
        displayed = sorted((v for v in fresh if v["score"] is not None), key=lambda v: float(v["score"]), reverse=True)[:3]
        if not displayed:
            details = "no scored fresh results"
        else:
            details = "; ".join(
                f"{v['pair']}:{float(v['score']):+.3f} | {v['features'].get('reason', '')}"
                for v in displayed
            )
        lines.append(f"{node:<20} {len(fresh):>4}/{len(values):<8} {details}")
    return lines


def _leaderboard_lines(connection: sqlite3.Connection) -> list[str]:
    try:
        row = connection.execute(
            "SELECT timestamp_ms, payload FROM trade_records WHERE kind='candidate_cycle' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        return ["No scanner-cycle records yet. Restart the scanner after installing the monitor update."]
    if row is None:
        return ["Waiting for the next scanner cycle (default cadence: 60 seconds)."]
    try:
        payload = json.loads(row["payload"])
    except (TypeError, json.JSONDecodeError):
        return ["Latest scanner-cycle record could not be decoded."]
    age = max(0.0, (time.time() * 1000 - int(row["timestamp_ms"])) / 1000)
    lines = [
        f"Cycle: {_local_time(row['timestamp_ms'])} ({age:.0f}s ago)",
        f"Fresh tickers: {payload.get('fresh_ticker_count', 0)}/{payload.get('snapshot_count', 0)}",
    ]
    candidates = payload.get("candidates", [])
    if not candidates:
        lines.append("No candidates passed the current filters in the last cycle.")
        if not payload.get("fresh_ticker_count"):
            lines.append("Reason: no fresh ticker input; this is a feed-health issue, not a model rejection.")
        return lines
    lines.append("RANK  SIDE   PAIR             SCORE   PRICE       LIQUIDITY   REASONS")
    ordered = sorted(candidates, key=lambda item: float(item.get("score", 0)), reverse=True)
    for index, item in enumerate(ordered, 1):
        features = item.get("features", {})
        reasons = ", ".join(str(reason) for reason in item.get("reasons", []))
        lines.append(
            f"{index:>4}  {str(item.get('side','?')).upper():<5}  {item.get('pair','?'):<15} "
            f"{float(item.get('score', 0)):.3f}   {features.get('price', '-')!s:<10} "
            f"{features.get('liquidity', '-')!s:<10} {reasons}"
        )
    return lines


def _model_lines(connection: sqlite3.Connection) -> list[str]:
    try:
        rows = connection.execute(
            "SELECT timestamp_ms, payload FROM model_reviews ORDER BY id DESC LIMIT 8"
        ).fetchall()
    except sqlite3.Error:
        return ["No model-review table yet."]
    if not rows:
        return ["No Gemini reviews yet. Empty candidate cycles do not call Gemini."]
    lines = ["TIME                 STATUS       DETAILS"]
    for row in rows:
        try:
            payload = json.loads(row["payload"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        status = payload.get("status", "recorded")
        details = payload.get("reason", "")
        if not details:
            details = ", ".join(
                str(review.get("pair", "")) + ":" + str(review.get("decision", ""))
                for review in payload.get("reviews", [])
            ) or ", ".join(payload.get("candidate_pairs", []))
        lines.append(f"{_local_time(row['timestamp_ms']):<20} {status:<12} {details}")
    return lines


def _trade_lines(connection: sqlite3.Connection) -> list[str]:
    try:
        rows = connection.execute(
            "SELECT timestamp_ms, kind, payload FROM trade_records "
            "WHERE kind!='candidate_cycle' ORDER BY id DESC LIMIT 12"
        ).fetchall()
    except sqlite3.Error:
        return ["No trade-record table yet."]
    if not rows:
        return ["No paper signals, risk decisions, orders, or outcomes recorded yet."]
    lines = ["TIME                 EVENT                    DETAIL"]
    for row in reversed(rows):
        try:
            payload = json.loads(row["payload"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        pair = payload.get("pair", "")
        side = payload.get("side", "")
        reason = payload.get("reason", payload.get("exit_reason", payload.get("note", "")))
        detail = " ".join(str(value) for value in (side, pair, reason) if value)
        lines.append(f"{_local_time(row['timestamp_ms']):<20} {row['kind']:<24} {detail}")
    return lines


def _render(view: str, db_path: Path, sections: list[tuple[str, list[str]]]) -> None:
    if os.name == "nt":
        clear = "\033[2J\033[H"
    else:
        clear = "\033[2J\033[H"
    output = [clear, f"COINDCX FUTURES LIVE MONITOR  |  view={view}  |  {_local_time(int(time.time()*1000))}",
              f"Database: {db_path}  |  Read-only monitor  |  Press Ctrl+C to close this view only", ""]
    for title, lines in sections:
        output.append(f"=== {title} ===")
        output.extend(lines)
        output.append("")
    print("\n".join(output), end="\n", flush=True)


def _local_time(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(int(timestamp_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")
