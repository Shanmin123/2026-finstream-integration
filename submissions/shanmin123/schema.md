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
