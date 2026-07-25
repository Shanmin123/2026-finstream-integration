# Output Schemas

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
| `extra_data` | string | Optional headline metadata; null when unavailable |

Final partition path:
`news/event_date=YYYY-MM-DD/symbol=SYMBOL/part-000.parquet`.

Article body text is not part of this schema.

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
| `rsi_14` | double | Wilder RSI over 14 sessions |
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
| `symbol` | string | Requested stock symbol |
| `field` | string | `last`, `bid`, `ask`, `close`, `high`, `low`, `last_size`, `bid_size`, `ask_size`, `volume` |
| `value` | double | Tick value |
| `retrieved_at_utc` | string | Stream-batch write timestamp, ISO 8601 UTC |

Final partition path: `quotes/event_date=YYYY-MM-DD/symbol=SYMBOL/part-000.parquet`.
Collected with `reqMktData` market data type 3 (delayed, the free paper tier,
15-20 minutes behind); with live market-data subscriptions the same command
streams real time (`stream.market_data_type: 1`).

## Live Feature Parquet (provisional intraday)

`indicators_live` and `alphas_live` carry the same columns as the daily datasets
plus `as_of_utc` (refresh time) and `provisional` (always true). Each row is the
latest intraday value: the newest streamed `last` price is appended to the daily
series as a provisional bar (open=high=low=close=last, volume 0) and the daily
formulas are recomputed. Values converge to the final daily rows once the real
bar is collected. Partition path mirrors the daily datasets
(`indicators_live/event_year=YYYY/symbol=SYMBOL/part-000.parquet`); dedup key
`(symbol, event_date)` keeps the newest refresh.
