from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from dataclasses import asdict
from pathlib import Path

from .config import load_dotenv, load_settings
from .engine import CoinDCXFramework
from .replay import replay_market_events


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local CoinDCX INR futures scanner with paper-first execution")
    parser.add_argument("--config", type=Path, default=Path("config.toml"), help="TOML settings file")
    parser.add_argument("--db", type=Path, default=Path("data/audit.sqlite3"), help="Local SQLite audit database")
    parser.add_argument("--arm-live", action="store_true", help="Arm live execution; config must also set execution.mode='live'")
    parser.add_argument("--validate-config", action="store_true", help="Validate configuration and exit without network calls")
    parser.add_argument("--replay", action="store_true", help="Replay recorded market events from the audit database; never calls APIs or places orders")
    parser.add_argument(
        "--monitor", choices=("all", "market", "analysis", "leaderboard", "model", "trades"),
        help="Open a read-only live terminal monitor for a running scanner",
    )
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    load_dotenv(args.config.parent / ".env")
    try:
        settings = load_settings(args.config)
        if args.replay:
            if args.validate_config:
                raise ValueError("--replay and --validate-config are separate commands")
            if args.arm_live:
                raise ValueError("--replay cannot be combined with --arm-live")
            if args.monitor:
                raise ValueError("--replay and --monitor are separate commands")
            event_count, candidates = asyncio.run(replay_market_events(args.db, settings))
            print(json.dumps({
                "mode": "offline_replay",
                "market_events_replayed": event_count,
                "candidates": [asdict(candidate) for candidate in candidates],
            }, indent=2, allow_nan=False))
            return 0
        if args.monitor:
            if args.validate_config or args.arm_live:
                raise ValueError("--monitor cannot be combined with --validate-config or --arm-live")
            from .monitor import run_monitor
            run_monitor(args.db, args.monitor, settings.stale_after_seconds)
            return 0
        if args.validate_config:
            if args.arm_live:
                _validate_live_prerequisites(settings)
            print("Configuration is valid. Live mode is inactive unless --arm-live is also used.")
            if settings.model_enabled and not os.getenv("GEMINI_API_KEY"):
                print("GEMINI_API_KEY is not set; scanning can run, but model review will fail closed.")
            return 0
        framework = CoinDCXFramework(args.config, args.db, arm_live=args.arm_live)
        asyncio.run(framework.run())
    except KeyboardInterrupt:
        print("Stopped.")
        return 130
    except Exception as exc:
        logging.getLogger(__name__).error("Startup failed: %s", exc)
        return 2
    return 0


def _validate_live_prerequisites(settings) -> None:
    if settings.execution_mode != "live":
        raise ValueError("Set execution.mode='live' in config.toml before passing --arm-live")
    if not settings.risk_limits.configured():
        raise ValueError("All five hard risk limits must be positive before live mode can arm")
    if not os.getenv("COINDCX_API_KEY") or not os.getenv("COINDCX_API_SECRET"):
        raise ValueError("Set COINDCX_API_KEY and COINDCX_API_SECRET in .env")
    if not os.getenv("GEMINI_API_KEY"):
        raise ValueError("Set GEMINI_API_KEY; live entries fail closed without model review")


if __name__ == "__main__":
    raise SystemExit(main())
