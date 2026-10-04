# CoinDCX INR Futures Framework

A local, event-driven scanner for CoinDCX INR-margined perpetual futures. Market events update a shared versioned state and fan out to independent analysis workers. The scanner ranks candidates and can ask the direct Gemini Developer API for a structured review. API or data failures never authorize a new entry.

The starter configuration is paper-only. Live mode requires both `execution.mode = "live"` and the separate `--arm-live` command-line flag, private CoinDCX credentials, and all five hard risk limits. The private execution adapter uses isolated margin, confirms a filled position, and attaches stop-loss and take-profit market protection before it will continue. If protection fails, it attempts an emergency exit and halts new entries.

## Cost and account notes

- No paid data or model provider is configured. CoinDCX public futures data is used for instruments, current prices, books and streaming candles. The current-price feed provides funding-rate fields; open interest is unavailable and is excluded.
- Gemini calls use the official Developer API and a free-tier API key. Google AI Pro CLI/Antigravity sign-in is not used as a third-party model adapter. Free-tier prompts may be used to improve Google products, so the model receives public market snapshots only.
- Use an AI Studio project that has no billing account linked. The program does not select paid models or use paid grounding, but cannot inspect the billing status of the project behind an API key. Free-tier quotas can change; exhaustion blocks review and entries.
- CoinDCX trading fees and funding costs may apply to live positions even though no paid API service is used.

## Setup

Use Python 3.11 or newer.

```sh
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp config.example.toml config.toml
cp .env.example .env
```

Add a Gemini Developer API free-tier key to `.env` as `GEMINI_API_KEY`. The key must belong to a project without billing enabled. Do not put exchange credentials into Gemini prompts or commit `.env` / `config.toml`.

Validate offline, without contacting CoinDCX or Google:

```sh
python -m coindcx_futures --validate-config
```

Start scanning in paper mode:

```sh
python -m coindcx_futures
```

The service discovers active INR futures instruments, selects up to `market.max_symbols` by current 24-hour volume (including BTC), and starts CoinDCX Socket.IO futures subscriptions. It seeds one-minute candles at startup and refreshes 50-deep books through a bounded public REST fallback. Analysis weights and scanner thresholds are hot-reloaded from `config.toml`; changing the universe or subscribed timeframes requires a restart. Events, node outputs, reviews, signals, outcomes and order records are stored in `data/audit.sqlite3`.

Replay the recorded market feed offline, without CoinDCX/Gemini API calls or orders:

```sh
python -m coindcx_futures --replay
```

Replay reports the latest candidates available in the recorded event history. It uses the scanner settings from `config.toml` and never evaluates or executes live orders.

## Live terminal monitors

Keep the scanner running in its own terminal. Open other terminals in this project folder and start read-only views against the same SQLite database:

```sh
python -m coindcx_futures --monitor market
python -m coindcx_futures --monitor analysis
python -m coindcx_futures --monitor leaderboard
python -m coindcx_futures --monitor model
python -m coindcx_futures --monitor trades
```

Use `--monitor all` for a combined view. Each monitor refreshes once per second; Ctrl+C closes only that monitor. The views show market freshness, latest node scores, scanner-cycle candidates, model review status, and paper/live audit events. Candidate-cycle history begins when the scanner is restarted with this version; other panels can read existing audit events immediately. The monitor is read-only and does not control the scanner or submit orders.

## Live mode

Live execution is intentionally not ready in the example configuration. Review CoinDCX API permissions and set trade-only credentials in `.env`; do not grant withdrawal permissions. Set `execution.mode = "live"` and all five `[risk]` values to positive INR limits, then inspect the config and run:

```sh
python -m coindcx_futures --validate-config --arm-live
python -m coindcx_futures --arm-live
```

`--arm-live` is required on every run. The risk gate computes maximum quantity from stop distance, contract value, the CoinDCX INR/USDT conversion rate, total exposure and configured limits. It rejects missing account data, stale prices, invalid protection levels, duplicate positions, low model confidence, and any proposal outside the configured caps. If an order may have reached CoinDCX but its state cannot be confirmed, the process halts entries for manual reconciliation.

## Architecture

- `CoinDCXSocketFeed` sends market events to the single state-ingestion actor.
- `MarketState` rejects out-of-order updates, versions per-instrument snapshots, and retains recent candles.
- `EventBus` fans each accepted update to BTC regime, liquidity, momentum, multi-timeframe trend, relative strength, and funding/positioning nodes concurrently.
- The ranker emits at most three LONG and three SHORT candidates per review cycle. Gemini receives one batch of public data and returns schema-constrained JSON; application validation still runs before the result is used.
- SQLite records market events, node outputs, model reviews, risk decisions and execution outcomes for audit.

Open interest and CoinDCX participant positioning are recorded as unavailable in this first version. No signal is inferred from missing data.

Paper signals are followed against subsequent public last-price updates. Stop/target exits and gross price returns are stored and recent paper outcome summaries are added to later model reviews. These paper returns exclude trading fees, funding and slippage.
