# Validation evidence

Numbers below come from the delivered datasets and a live IB Gateway paper
session, not from fixtures. Reproduce with the commands in `README.md`.

## Delivered batch datasets

| Dataset | Rows | Partitions | Notes |
| --- | ---: | ---: | --- |
| `prices` | 814,022 | 3,900 | 658 symbols, 2021-07-26 to 2026-07-24 |
| `indicators` | 814,022 | 3,900 | 1:1 with prices on `(symbol, event_date)` |
| `alphas` | 814,022 | 3,900 | 1:1 with prices on `(symbol, event_date)` |
| `news` | 200 | 102 | IBKR headlines, complementary to the primary news feed |

Integrity checks over the full panel (not a sample):

* 0 duplicate `(symbol, event_date)` rows in any dataset.
* 0 null/infinite values in daily OHLCV; no non-positive prices; no `high < low`;
  no close outside `[low, high]`.
* 0 infinities in indicators and alphas. (An earlier build had 9,984 `±inf`
  `alpha_53` values across 601 symbols where `close == low` made the denominator
  zero; degenerate denominators now yield NaN and the datasets were regenerated.)
* Indicator warm-up NaNs only, and only where the window requires them; defined
  RSI spans [0.63, 98.20] on the delivered panel (a flat 14-session window
  would be 0/0 and stays NaN; none occurs in this panel), Bollinger bands
  never invert, ATR is never negative.

## Live Gateway verification

* `smoke-test`: connects, returns 7 daily bars and the 8 subscribed news
  providers.
* `collect-prices` over the full universe: 673 requested, 658 collected;
  the 15 remaining are the delisted/renamed names documented in the README,
  whose contract resolution fails with IB error 200 as expected. 0
  unexpected failures after the share-class symbol mapping; 814,022 rows.
* `stream-pipeline`: tick-driven flushes observed (12 ticks then 4 ticks in
  consecutive intervals, no writes during a quiet interval), clean shutdown.
* Live recomputation cost on the 658-symbol panel: 0.11s for 2 moved symbols,
  0.88s for 20, 4.2s for 100 — inside the default 5s flush interval.

## Test suite

125 tests green under the production contract (Python 3.8-compatible pins,
pandas 1.3.5, numpy 1.22.4, pyarrow 12.0.1), and every source file compiles
under Python 3.8.20. PySpark 3.5.1 + Java 17 reads a written partition of all
seven final datasets (prices, news, indicators, alphas, quotes, and both live
feature sets); read whole datasets by file glob as documented in `schema.md`.

## Platform interface alignment

Checked line by line against the platform-side code in the integration repo
(`submissions/emaad-ahmad`, the team's EODHD/platform loaders), not against
assumptions:

* PostgreSQL env vars (`POSTGRES_HOST/PORT/USER/PASSWORD/DB`) and their
  defaults (host `postgres`, user `finplatform`, db `financial_data`) —
  identical, except `POSTGRES_PASSWORD`, which this feed defaults to empty
  instead of embedding the platform's dev password; supply it via the
  environment.
* Universe query — the same statement verbatim
  (`SELECT ticker FROM companies WHERE is_active = TRUE ORDER BY ticker`).
* `price_data` insert column order, `ON CONFLICT (ticker, timestamp_ms,
  interval) DO NOTHING`, and the `'1d'` interval literal — identical.
* Daily `timestamp_ms` — both feeds compute UTC midnight of the trading date
  with the same expression, so the gap-filler conflict key genuinely matches.
* `pipeline_runs` telemetry columns (dag_id, start_time, end_time,
  records_ingested, latency_seconds, status, error_message) — identical.
* Scheduling — the EODHD daily DAG documents `0 4 * * 2-6`; this feed's DAG
  docstring recommends `0 6 * * 2-6` so the gap filler runs after it.
* News hand-off — platform news lives in MongoDB `news_articles` (the other
  submission's scope); this feed keeps its complementary IBKR headlines in
  parquet and does not write MongoDB.

## Known data characteristics, recorded rather than hidden

* **2,281 zero-volume sessions** and 554 zero-`bar_count` rows, concentrated in
  illiquid or delisted names.
* **24 symbols have fewer than the full 1,255 sessions.** Most are recent
  listings or spin-offs; `ODFL` (from 2024-05-07) and `COO` (from 2023-09-26)
  reflect IBKR contract-history boundaries rather than the company's trading
  history.
* **Five non-holiday gaps**: EXE 2024-10-02, EXPD 2023-11-21, FISV 2023-06-07,
  FITB 2026-06-11, TMC 2021-09-10.
* **Stale or recycled tickers.** The universe is a symbol snapshot, not a dated
  membership table, so names such as `COMS`, `SBNY` and `UCL` carry
  post-delisting or recycled-ticker prices (last close under \$1). A renamed
  company also appears under its old symbol: Fiserv is stored as `FISV`, while
  the platform's `companies` refresh uses the current `FI`, so a future run
  taking its universe from the platform would split that instrument. Resolving
  this needs a dated ticker-to-entity (or conId) alias table; until then treat
  the historical panel as symbol-keyed, not entity-keyed.
* **`alpha_20` is survivorship-affected in backfills** for the same reason: its
  cross-sectional rank uses whichever symbols are in the panel. Daily
  forward-computed values are unaffected.
