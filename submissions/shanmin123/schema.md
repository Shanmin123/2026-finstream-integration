# Output Schemas

Every column lives in the data files themselves; the `event_year=`/`event_date=`
and `symbol=` directory names organise the layout and are not needed to
reconstruct any column. Read the parquet datasets by file path or glob, e.g.
`spark.read.parquet(f"{root}/event_year=*/symbol=*/part-000.parquet")`, rather
than pointing Spark at the bare dataset root: hive-style partition discovery
would derive a `symbol` column from the directory names that collides with the
`symbol` column already present in the files.

## Raw JSONL Envelope

Each raw file contains one JSON object and a trailing newline.

| Field | Type | Description |
| --- | --- | --- |
| `request` | object | Symbol, window, providers, and request parameters |
| `retrieved_at_utc` | string | ISO 8601 UTC retrieval timestamp |
| `records` | array<object> | Normalised records returned from TWS API callbacks |

## Final Prices Parquet

Deduplication key: `(symbol, event_date)`.

| Column | Spark type | Description |
| --- | --- | --- |
| `event_date` | string | Trading-session date in `YYYY-MM-DD` format |
| `symbol` | string | Requested stock symbol |
| `con_id` | long | Qualified IBKR contract identifier |
| `exchange` | string | Qualified routing exchange, normally `SMART` |
| `currency` | string | Contract currency |
| `open` | double | Daily opening trade price |
| `high` | double | Daily high trade price |
| `low` | double | Daily low trade price |
| `close` | double | Daily closing trade price |
| `volume` | double | IBKR historical bar volume |
| `bar_count` | long | Number of trades represented by the bar |
| `wap` | double | Weighted average price reported by IBKR |
| `retrieved_at_utc` | string | ISO 8601 UTC retrieval timestamp |

Final partition path:
`prices/event_year=YYYY/symbol=SYMBOL/part-000.parquet`.

## Final News Parquet

Deduplication key: `(symbol, provider_code, article_id)`.

| Column | Spark type | Description |
| --- | --- | --- |
| `published_at_utc` | string | Headline timestamp normalised to ISO 8601 UTC |
| `retrieved_at_utc` | string | ISO 8601 UTC retrieval timestamp |
| `symbol` | string | Stock symbol used to resolve the IBKR contract |
| `con_id` | long | Qualified IBKR contract identifier |
| `provider_code` | string | IBKR news provider code |
| `article_id` | string | Provider-specific article identifier |
| `headline` | string | Headline with IBKR metadata tags removed |

Final partition path:
`news/event_date=YYYY-MM-DD/symbol=SYMBOL/part-000.parquet`.

Article body text is not part of this schema.

IBKR historical news paginates BACKWARDS: `reqHistoricalNews` returns up to
`limit` headlines at or before its anchor and flags `hasMore` when older ones
remain. The collector anchors each run at the current time and follows that
flag page by page, re-anchoring at the oldest time received, until the
checkpoint minus the configured overlap is reached — so a burst larger than one
page between runs is not lost. The `(symbol, provider_code, article_id)` dedup
absorbs the boundary article each next page re-returns. A deeper backfill is
the same walk with an older stop time. Availability on the default free
provider set lags: at the time of collection the newest AAPL headline from
BRFG/BRFUPDN/DJNL was about three months old, so this feed complements rather
than replaces the primary news source.

## Final Indicators Parquet

Derived offline from the final prices dataset (`compute-features`).
Deduplication key: `(symbol, event_date)`.

| Column | Spark type | Description |
| --- | --- | --- |
| `symbol` | string | Stock symbol |
| `event_date` | string | Trading-session date in `YYYY-MM-DD` format |
| `sma_20`, `sma_50` | double | Simple moving averages of close |
| `ema_12`, `ema_26` | double | Exponential moving averages of close |
| `macd`, `macd_signal`, `macd_hist` | double | MACD(12,26,9) line, signal, histogram |
| `rsi_14` | double | Wilder RSI over 14 sessions; a window with zero gains and zero losses (flat prices) is 0/0 and stays NaN |
| `bb_mid_20`, `bb_upper_20`, `bb_lower_20` | double | Bollinger Bands (20, 2 std) |
| `atr_14` | double | Wilder Average True Range over 14 sessions |
| `obv` | double | On-Balance Volume (cumulative) |
| `computed_at_utc` | string | ISO 8601 UTC computation timestamp |

Final partition path:
`indicators/event_year=YYYY/symbol=SYMBOL/part-000.parquet`.

## Final Alphas Parquet

Alpha101 subset (Kakushadze 2015), pandas-only, computed offline from final
prices; formulas are documented inline in `pipeline/features.py`.
Deduplication key: `(symbol, event_date)`.

| Column | Spark type | Description |
| --- | --- | --- |
| `symbol` | string | Stock symbol |
| `event_date` | string | Trading-session date in `YYYY-MM-DD` format |
| `alpha_6` | double | `-corr(open, volume, 10)` |
| `alpha_9` | double | Signed close-delta with 5-day trend filter |
| `alpha_12` | double | `sign(delta(volume,1)) * -delta(close,1)` |
| `alpha_20` | double | Cross-sectional rank alpha; ranks span the symbols present that date (single-symbol runs degenerate to the constant -1) |
| `alpha_23` | double | Conditional high-delta vs 20-day mean high |
| `alpha_53` | double | `-delta(((close-low)-(high-close))/(close-low), 9)` |
| `alpha_54` | double | `-((low-close)*open^5) / ((low-high)*close^5)` |
| `alpha_101` | double | `(close-open) / ((high-low)+.001)` |
| `computed_at_utc` | string | ISO 8601 UTC computation timestamp |

Final partition path:
`alphas/event_year=YYYY/symbol=SYMBOL/part-000.parquet`.

The full 101-alpha set (external `alpha101` package, Python 3.13 environment)
and Qlib Alpha158 remain in the research repository; this submission ships the
production-contract-compatible subset.

## Final Quotes Parquet (streaming)

Deduplication key: `(symbol, tick_time_utc, field)`.

| Column | Spark type | Description |
| --- | --- | --- |
| `tick_time_utc` | string | Tick receipt time, ISO 8601 UTC |
| `session_date` | string | Exchange-local trading session (America/New_York), which is what the partition uses: an extended-hours tick received after UTC midnight still belongs to the previous session |
| `symbol` | string | Requested stock symbol |
| `field` | string | `last`, `bid`, `ask`, `open`, `close`, `high`, `low`, `last_size`, `bid_size`, `ask_size`, `volume`. Note `close` is IB's PREVIOUS-session close and is never used as the live price |
| `value` | double | Tick value |
| `retrieved_at_utc` | string | Stream-batch write timestamp, ISO 8601 UTC |

Final partition path: `quotes/event_date=YYYY-MM-DD/symbol=SYMBOL/part-000.parquet`,
partitioned on `session_date`.
Collected with `reqMktData` market data type 3 (delayed, the free paper tier,
15-20 minutes behind); with live market-data subscriptions the same command
streams real time (`stream.market_data_type: 1`).

## Live Feature Parquet (provisional intraday)

`indicators_live` and `alphas_live` carry the same columns as the daily datasets
plus `as_of_utc` (refresh time) and `provisional` (always true). Each row is the
latest intraday value: the streamed `last` price becomes the provisional close
and the daily formulas are recomputed. Fields the stream has not reported
(open/high/low/volume) stay unknown rather than being filled with the last
price, so any value that needs them — `atr_14`, `obv`, and the range-based
alphas — is NaN until the corresponding tick arrives. Values converge to the final daily rows once the real
bar is collected. Partition path mirrors the daily datasets
(`indicators_live/event_year=YYYY/symbol=SYMBOL/part-000.parquet`); dedup key
`(symbol, event_date)` keeps the newest refresh.

## Platform PostgreSQL tables

DDL: `pipeline/db/init_postgres/ibkr_tables.sql`. All rows carry `source='ibkr'`.

| Table | Key | Conflict policy |
| --- | --- | --- |
| `price_data` (platform's own) | `(ticker, timestamp_ms, interval)` | DO NOTHING — the constraint excludes `source`, so an IBKR bar cannot coexist with a primary-feed bar for the same ticker/day. This feed therefore fills gaps and is a no-op elsewhere; the writer logs how many rows actually landed. Coexistence would need `source` added to that constraint by the table's owner. |
| `quote_ticks` | `(ticker, timestamp_ms, field, source)` | DO NOTHING (immutable observations); `market_data_type` records 1 = real time, 3 = delayed |
| `technical_indicators` | `(ticker, timestamp_ms, indicator_name, interval, source)` | DO UPDATE when `computed_at_utc` is newer |
| `alpha_factors` | `(ticker, timestamp_ms, alpha_id, interval, source)` | DO UPDATE when `computed_at_utc` is newer |

The derived tables are long `(name, value)` rather than one column per
indicator, matching this project's existing collectors: adding an indicator or
an alpha formula needs no migration. `is_provisional` marks an intraday value
computed from a streamed price; the end-of-day run overwrites it in place.

Symbols are stored in one canonical form: upper-cased, with share-class dots
written as dashes (`BRK.B` and `BRK-B` both store as `BRK-B`), because both
resolve to the same IB contract and two spellings would split an instrument's
partitions. Every universe (prices, news, stream, and tickers read from the
platform) passes through that form.

Known limits, recorded rather than hidden:

* `quote_ticks` keys on millisecond timestamps, so two updates to the same field
  inside one millisecond collide in PostgreSQL (both are kept in parquet).
* `alpha_20` is cross-sectional: its rank is taken over whichever symbols are in
  the panel. The symbols file is a snapshot of names trading at one date, not
  dated membership, so a multi-year backfill ranks past dates against a later
  constituent set. Historical `alpha_20` is therefore survivorship-affected;
  removing that needs a membership table with effective dates. The daily
  forward-computed values are unaffected.
* Each flush rewrites the affected symbol/day quote partition, so per-flush cost
  grows with the day's tick count for a symbol.
* The live recomputation is scoped to the symbols that ticked in that interval,
  over their FULL history: OBV is cumulative and EMA/MACD and Wilder RSI/ATR are
  recursive, so a bounded warm window would silently change their values.
  Measured on the 658-symbol panel: 0.11s for 2 moved symbols, 0.88s for 20,
  4.2s for 100. If nearly the whole universe ticks inside one interval the work
  grows proportionally, so a large universe wants a flush interval sized to it.
