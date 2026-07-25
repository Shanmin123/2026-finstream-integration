# IBKR Gateway Data Pipeline

## Features

This pipeline collects two IBKR datasets and derives two feature datasets:

- Daily stock OHLCV bars from `reqHistoricalData`
- Historical news headlines and provider metadata from `reqHistoricalNews` and
  `reqNewsProviders`
- Technical indicators (SMA/EMA, MACD, RSI, Bollinger, ATR, OBV) computed
  offline from the collected prices
- An Alpha101 subset (8 documented formulas, pandas-only) computed offline from
  the collected prices

It resolves stock contracts through IBKR, stores raw request/response records,
normalises and deduplicates the data, writes partitioned Parquet outputs, and
resumes from durable checkpoints. JSON logs record connections, requests,
retries, row counts, and errors.

## Connection Method

The pipeline uses the official IBKR TWS API through IB Gateway:

```text
Python 3.8 pipeline
  -> ibapi 9.81.1.post1
  -> TCP socket at 127.0.0.1:4002
  -> IB Gateway paper-trading session
  -> IBKR backend
  -> raw JSONL, intermediate Parquet, final Parquet
```

IBKR credentials are entered only in IB Gateway. They are not placed in the
YAML file or passed to Python.

Unlike the team's key-based cloud APIs (EODHD, GDELT), this is a local-session
API: IBKR's hosted Web API requires a funded IBKR Pro account, so the free
paper route runs against a logged-in Gateway on the collector host. The
integration hand-off is therefore the pipeline's partitioned parquet output
(synced to OneDrive / the shared database), not a callable remote endpoint.

## Environment

The code and tests use Python 3.8 syntax and the library versions in
[`environment-constraints.txt`](environment-constraints.txt). Direct pipeline
dependencies are pinned in [`requirements.txt`](requirements.txt).

```bash
python3.8 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

On Windows, activate the environment with:

```powershell
.venv\Scripts\Activate.ps1
```

## IB Gateway Setup

1. Install [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-latest.php).
2. Sign in with the IBKR paper-trading username.
3. Open **Configure > Settings > API > Settings**.
4. Enable socket clients and set the paper API port to `4002`.
5. Keep **Read-Only API** enabled for this data-only pipeline.
6. Add `127.0.0.1` as a trusted IP when the collector runs on the same host.
7. Keep IB Gateway running while executing collection commands.

Use a unique `client_id` if another TWS API program is connected. For a remote
deployment, set `ib.host` to the Gateway host and configure its trusted-IP and
network controls before running the collector.

## Configuration

From this submission directory:

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` to set the Gateway host, port, client ID, symbols, windows,
output paths, and retry policy. `config.yaml` is ignored by Git. The connection
fields can also be overridden at runtime with `IB_HOST`, `IB_PORT`, and
`IB_CLIENT_ID`.

The sample stock list is illustrative. Replace `symbols.example.csv` with the
approved universe file before a full collection run.

## Commands

Run the commands from `submissions/shanmin123` with IB Gateway open:

```bash
python -m pipeline.main --config config.yaml smoke-test
python -m pipeline.main --config config.yaml collect-prices
python -m pipeline.main --config config.yaml collect-news
python -m pipeline.main --config config.yaml run-all
python -m pipeline.main --config config.yaml compute-features
python -m pipeline.main --config config.yaml stream-pipeline
python -m pipeline.main --config config.yaml stream-quotes
python -m pipeline.main --config config.yaml compute-live-features
```

`smoke-test` performs read-only AAPL-style price and news checks without writing
data or advancing checkpoints. `run-all` opens one Gateway connection and runs
both production collectors. `compute-features` needs no Gateway: it reads the
final prices dataset and writes the indicators and alphas datasets.
`stream-pipeline` is the continuous mode and the one to run in production: a
single long-lived process that loads the daily history once as warm state,
subscribes to quote ticks, and on every `stream.flush_interval_seconds` appends
the buffered ticks to the quotes dataset and recomputes the provisional live
features for the symbols that moved. Arriving ticks drive the work — a quiet
interval writes nothing. Set `stream.duration_seconds: 0` to run until stopped
(Ctrl-C shuts down cleanly: the subscription is cancelled and pending ticks are
flushed). Writes are micro-batched at the flush interval rather than per tick
because the sink is partitioned parquet.

The free paper tier streams delayed data (15-20 minutes behind); with
market-data subscriptions the same process streams real time
(`stream.market_data_type: 1`) with no code change.

`stream-quotes` and `compute-live-features` run the same two stages standalone
(capture-only for a fixed window, and an offline feature refresh from persisted
state that needs no Gateway). They share the pipeline's code and are useful for
backfill and testing.

## Platform integration

Airflow DAGs live in `pipeline/airflow/dags/`:

* `ibkr_prices_dag.py` — daily collection, feature derivation, and an upsert of
  the bars into the platform's PostgreSQL `price_data` table with
  `source='ibkr'` and the platform's own conflict key
  `(ticker, timestamp_ms, interval)`. The universe is read from
  `companies.is_active` so the DAG does not hardcode a list, falling back to the
  configured symbols file when the database is unreachable.
* `ibkr_stream_dag.py` — the continuous streaming pipeline as a bounded task.

Both are `schedule=None` pending lab bring-up, and point at
`IBKR_PIPELINE_CONFIG` (default `/opt/airflow/config/ibkr.yaml`). psycopg2 is an
optional dependency: without a database the pipeline still produces every
parquet dataset.

Platform tables are created by `pipeline/db/init_postgres/ibkr_tables.sql`:
`quote_ticks` (immutable ticks, conflicts ignored as with `price_data`),
`technical_indicators` and `alpha_factors` (long `(name, value)` layout, so new
indicators or alpha formulas need no migration; the upsert keeps the newer
computation, which is what lets an intraday `is_provisional = TRUE` row be
replaced in place by the end-of-day value). Daily bars go to the platform's
existing `price_data` with `source='ibkr'`.

## Universe note

`symbols.sp500.csv` is a point-in-time snapshot (S&P 500 membership 2000-2025,
symbols still trading at 2025-12-31). As of 2026-07, 15 of the 673 names have
left the US market (delisted, acquired, or renamed) and have no US IB contract:
AL, ATGE, BK, CMA, CTRA, DAY, HI, HOLX, IAC, MMC, PCH, PX, SEE, SNV, TGNA.
The collector reports IB error 200 for them and continues; the live datasets
cover the remaining 658 names. Refresh the list to change the universe.

## Output

The configured directories contain:

```text
data/
  raw/
    prices/retrieval_date=YYYY-MM-DD/*.jsonl
    news/retrieval_date=YYYY-MM-DD/*.jsonl
  processed/
    prices/symbol=SYMBOL/part-000.parquet
    news/symbol=SYMBOL/part-000.parquet
  output/
    prices/event_year=YYYY/symbol=SYMBOL/part-000.parquet
    news/event_date=YYYY-MM-DD/symbol=SYMBOL/part-000.parquet
  checkpoint.json
logs/
  ibkr_pipeline.jsonl
```

Raw JSONL records include the request metadata, retrieval time, and returned API
records. The final schemas are documented in [`schema.md`](schema.md). News
article bodies are not requested or stored.

The included files in [`samples`](samples) contain synthetic records only. The
pipeline does not commit collected IBKR data to GitHub.

## Resume Behavior

Price checkpoints store the latest durable `event_date` for each symbol. News
checkpoints store the latest durable `published_at_utc`. The checkpoint advances
only after raw and Parquet writes complete.

On restart, prices request the missing daily window with a three-day overlap.
News restarts from the last headline time with the configured minute overlap.
Partition writes then deduplicate prices by `(symbol, event_date)` and news by
`(symbol, provider_code, article_id)`.

## Testing

Install development dependencies and run the Python 3.8 suite:

```bash
python -m pip install -r requirements-dev.txt
python scripts/verify_environment.py
python -m pytest -q
python -m compileall -q pipeline tests
```

The Spark compatibility test starts a local PySpark 3.5.1 session and reads the
generated final Parquet partitions. Java 17 must be active for that test.

After the offline tests pass, run the live read-only check:

```bash
python -m pipeline.main --config config.yaml smoke-test
```

Expected output reports the tested symbol, daily-bar row count, available news
provider codes, and headline row count.

## Troubleshooting

- Connection timeout: confirm Gateway is running, paper login is complete, the
  socket API is enabled, and host/port match `config.yaml`.
- Error `502`: check the API port, trusted IP, and local firewall.
- Client ID already in use: select another integer in `ib.client_id`.
- Error `162` or pacing messages: reduce the symbol rate, increase retry delay,
  and resume after the IBKR pacing window.
- Empty prices: verify the contract, requested period, and market-data
  entitlement available to the paper username.
- Empty provider list or headlines: run `smoke-test` and use only provider codes
  returned for the logged-in username.

IBKR documents historical bar parameters and limits in its
[Historical Bar Data](https://interactivebrokers.github.io/tws-api/historical_bars.html)
and [Historical Data Limitations](https://interactivebrokers.github.io/tws-api/historical_limitations.html)
pages. News provider availability and API-specific subscriptions are described
in the [TWS API News documentation](https://interactivebrokers.github.io/tws-api/news.html).
