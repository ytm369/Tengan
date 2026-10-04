# CoinDCX Futures Framework — Version Roadmap

## Purpose

Keep the implemented version and future work in one place. The goal is to build a measurable, paper-tested futures research and execution framework. A high win rate is not promised; changes must be supported by out-of-sample evidence and net results after costs.

## Version 0 — Event-driven paper-first framework

**Status: implemented; first runtime review found a market-feed issue.**

### Implemented

- Python `asyncio` event bus and versioned shared market state.
- CoinDCX INR futures instrument discovery, REST current-price bootstrap, REST order-book polling, candle history bootstrap, and Socket.IO subscriptions.
- Concurrent analysis nodes for BTC regime, liquidity, momentum, multi-timeframe trend, relative strength, and funding. Open interest is left unavailable rather than inferred.
- Continuous candidate ranking, capped at three long and three short candidates per review cycle.
- Direct Gemini Developer API structured review for shortlisted candidates. Model failure is fail-closed; model output cannot bypass deterministic risk checks.
- SQLite audit and offline event replay.
- Paper signal tracking with stop/target outcomes and basic historical outcome summaries.
- Paper mode by default. A separate, explicitly armed live path exists but is not enabled or live-tested.
- Example configuration leaves all live risk limits unset.

### What the first runtime observation established

The run visible in the terminal was active for about 30 minutes. The audit database contained:

- 7,770 market events at the inspection point: 30 ticker events, 1,920 candles, and 5,820 order-book events.
- All 30 ticker events were the one-time REST bootstrap at 20:19:45. No later ticker events were recorded.
- The candle bootstrap ended at 20:19. Later order-book events continued through REST polling.
- Zero model reviews, paper signals, or closed paper outcomes.

The candidate scanner requires a ticker newer than the configured 20-second freshness limit. After the startup tickers aged out, it correctly had no fresh snapshots to rank. Therefore, the repeated “No fresh candidates” message in this run does **not** establish that the strategy rejected valid setups; it shows the scanner had no fresh ticker input. Continuing to watch the same run will not resolve this.

### Known limits

- The observed Socket.IO session did not deliver live ticker or candle events to the audit log. The precise cause still needs instrumentation and verification.
- Current candidate scores are simple heuristics (for example, 24-hour return, short candle trend, relative strength, book spread/depth, and funding). They have no measured trading edge yet.
- Replay is a scanner replay, not a cost-aware historical trading backtest.
- Paper returns are gross price returns. They omit fees, funding, slippage, spread-crossing, partial fills, and latency.
- Trade outcome records contain exit reason and return, but there is no concise post-trade explanation or recurring-pattern report yet.
- The current configured model limit is one request per minute. There is no explicit daily Gemini request budget yet.

## Version 1 — Feed health, evidence, and trade journal

**Status: in progress. Read-only terminal monitor views and scanner-cycle logging are implemented; live ticker recovery and strategy evaluation remain planned.**

### V1.0 — Make market-data health observable and recoverable

**Implemented in the current working tree:** separate read-only terminal views for market health, node analysis, candidate leaderboard, Gemini reviews, trade records, and a combined view. The scanner now persists one candidate-cycle summary per review cycle. Restart the scanner to begin writing those cycle summaries.

**Still planned:**

1. Trace Socket.IO connect, subscription acknowledgements, received event names, pair resolution, and accepted/rejected event counts.
2. Add a bounded REST ticker polling fallback for selected instruments if the live socket ticker stream is absent or stale. Keep the public stream as the preferred low-latency source; de-duplicate and reject out-of-order events.
3. Extend the health view to show latest event ages and stale-data reasons per pair, with an explicit warning when snapshots cannot be ranked.
4. Expand candidate-cycle records to include the full funnel: fresh instruments, missing/stale nodes, liquidity rejects, score rejects, and shortlisted names/scores. Do not lower thresholds just to force a trade.
5. Add an explicit daily model-call cap and 429 cooldown. Keep model review batched and call it only when there are actual candidates.

### V1.1 — Build a realistic historical evaluator

1. Load free CoinDCX historical candles and saved market events through a reproducible offline interface.
2. Simulate entry, stop, target, and position sizing without look-ahead. Document candle cases where stop and target both fall inside one bar and use a conservative fill assumption.
3. Include configured exchange fees, funding, spread, slippage, and market-order fill assumptions. If a cost cannot be sourced, expose it as an explicit assumption rather than silently treating it as zero.
4. Compare against simple baselines, including no trade and basic momentum, so added complexity must demonstrate an improvement.
5. Report number of trades, win rate, average win/loss, net expectancy, profit factor, drawdown, exposure, turnover, and fees/funding. Keep a record of every strategy variation tested and reserve untouched walk-forward periods to reduce selection bias.

### V1.2 — Structured post-trade report and pattern feedback

For each closed paper trade, save a concise report with:

- Setup snapshot and feature values at decision time.
- Scanner direction, score, reasons, and node availability/freshness.
- Gemini decision, confidence, rationale, and proposed protection levels, if a review occurred.
- Entry, exit, duration, exit reason, gross return, and net return when cost data is available.
- Deterministic tags such as trend alignment, BTC regime, spread/depth quality, funding extreme, stale/missing inputs, and stop/target outcome.
- A short explanation that distinguishes observed evidence from possible causes; do not claim causation from a single trade.

Aggregate those reports into similar setup/error groups. Use those groups as evidence in later candidate reviews, not as automatic permission to change strategy or risk settings. Model-generated strategy changes must be versioned and tested in the historical evaluator before paper use.

### V1.3 — Provider and agent boundaries

- Keep direct Gemini Developer API as the initial candidate-review provider. The data filter reduces calls, but quota exhaustion remains possible; each project’s active limits must be checked and the application must stop new reviews safely at its configured budget.
- Hermes may be evaluated for offline research, scheduled summaries, and post-trade report drafting. Keep it outside the market tick loop and order authorization path; its memory is not the source of truth (SQLite is).
- Do not use Gemini CLI or Antigravity account OAuth as a third-party API adapter. Do not add OpenRouter or NVIDIA NIM as an assumed free production fallback. Any alternative provider requires verified terms, current quotas, JSON-schema compatibility, and separate paper evaluation.
- A provider failure or exhausted quota must never cause a trade. The local scanner can continue collecting and ranking; model-reviewed entries remain disabled until review is available.

### V1 acceptance checks

- After startup, selected instruments receive ticker updates often enough to stay within the configured freshness window; if not, health output identifies the source and cause.
- With REST fallback enabled, the scanner can continue ranking from fresh public ticker data when Socket.IO tickers are missing, without duplicating or reversing newer events.
- Candidate-funnel counts explain why a cycle has no shortlist.
- Empty shortlists trigger zero Gemini calls. Nonempty shortlists use one structured batch and respect both per-minute and daily caps.
- Paper reports reconcile each simulated signal with exactly one exit outcome and expose cost assumptions.
- Historical evaluation is deterministic, includes cost assumptions, and separates development from walk-forward evaluation periods.
- No live orders are used for V1 acceptance.

## Observation guidance

### After the V1 feed fix

- Observe **5 minutes** after startup as an operational smoke test. With a 10-second fallback poll and a 20-second freshness window, the health summary should show ticker updates arriving and staying fresh. This checks data flow, not profitability.
- A valid feed does not owe us a trade on a clock. If there are no candidates, use the funnel diagnostics to see whether the market did not meet the rules or a node is missing data. Do not relax thresholds merely to produce activity.

### To evaluate strategy performance

- Run the historical evaluator first, then a paper-forward pilot. Set the evaluation horizon and sample criteria before reviewing results; avoid stopping as soon as the first profitable trade appears.
- Use at least 30 days as an initial paper-forward checkpoint, and continue if there are too few closed trades or only one market regime. A small sample is inconclusive, not proof of success or failure.
- Judge net expectancy, drawdown, profit factor, and stability alongside win rate. A high win rate can still lose money when losses are larger than wins.
- Keep live execution disabled until the data path is verified and the strategy passes the predeclared out-of-sample and paper criteria. No test can guarantee future results.

## Later versions

Version 2+ can be proposed after Version 1 evidence exists. Potential work includes a read-only dashboard, broader free public data sources, carefully evaluated model diversity for research, and live-mode operational safeguards. These are not prerequisites for the V1 feed and backtest work.
