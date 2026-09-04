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

Run the commands from `submissions/shanmin-sun` with IB Gateway open:

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
`IBKR_PIPELINE_CONFIG` (default `/opt/airflow/config/ibkr.yaml`).

Deploying into the shared Airflow: the DAG tasks import this `pipeline` package,
so copying the two DAG files alone is not enough — the package has to be
importable by the workers. Either mount this submission directory and add it to
`PYTHONPATH`, or `pip install ".[postgres]"` from this directory into the worker image
(packaging metadata is in `pyproject.toml`; the `postgres` extra brings
psycopg2, which the DAGs' database-loading tasks import -- plain
`pip install .` still produces every parquet dataset but those tasks would
fail. The table DDL ships with the package, while the DAG files are copied
into Airflow's own dags directory rather than installed). The worker also needs network access to a logged-in IB
Gateway at `ib.host`/`ib.port`; the Gateway login itself is interactive and is
not automated here.

Source precedence: `price_data`'s unique key excludes `source`, so this feed is
a gap filler only if it runs after the primary EODHD load — see the cadence note
in `ibkr_prices_dag.py`. psycopg2 is an
optional dependency: without a database the pipeline still produces every
parquet dataset.

Platform tables are created by `pipeline/db/init_postgres/ibkr_tables.sql`:
`quote_ticks` (immutable ticks, conflicts ignored as with `price_data`),
`technical_indicators` and `alpha_factors` (long `(name, value)` layout, so new
indicators or alpha formulas need no migration; the upsert keeps the newer
computation, which is what lets an intraday `is_provisional = TRUE` row be
replaced in place by the end-of-day value). Daily bars go to the platform's
existing `price_data` with `source='ibkr'`.

## MongoDB

The platform stores its shared datasets in MongoDB, so `pipeline/mongo_sink.py`
is the MongoDB counterpart of the PostgreSQL sink. It writes the same four
datasets, under the same field names, with the same conflict semantics:
`price_data` and `quote_ticks` leave an existing document alone, and the two
derived collections accept a newer computation but never let a provisional
intraday value replace a final end-of-day one.

The two sinks cannot drift. Every document is built by zipping a field-name
tuple onto the row tuples `db_sink.py` already produces, so a mapper that grows
a column raises rather than storing a document with a value dropped off the
end.

News is not written to either store: the platform defines no news collection,
its news comes from the EODHD and GDELT pipelines, and this pipeline's news is
a two-ticker sample. It is delivered as parquet.

`scripts/load_to_mongo.py` moves the collected parquet into the database. It is
separate from the collector because the parquet output stays valid whether or
not a database is reachable, and a load can be re-run without re-contacting
IBKR.

```bash
pip install ".[mongo]"
python scripts/load_to_mongo.py --data-root data/output --dry-run   # counts only
python scripts/load_to_mongo.py --smoke-test                        # two documents
python scripts/load_to_mongo.py --data-root data/output             # the load
```

`--dry-run` reads and validates every file and reports what would be written
without opening a connection. Every write is keyed and idempotent, so an
interrupted load is resumed by running it again.

Run `--smoke-test` first against a database nobody here has reached before. It
writes two documents under a ticker no exchange lists, checks that a second
identical write adds nothing, removes exactly what it wrote, and reports. That
turns the first live contact into ten seconds rather than several million
documents.

`tests/test_live_mongo.py` runs the conflict policies against a real server and
skips when none is reachable, the same way `test_live_mixed_run.py` skips
without a Gateway. It has been run against MongoDB 7.0: the pipeline update is
accepted, a newer computation replaces an older one, an older one is rejected, a
provisional value cannot replace a final one, an existing bar is left alone and
the write reports nothing landed, dates are stored as dates, and the conflict
keys are enforced by unique indexes. A subset load of 25,736 documents was
re-run and changed nothing.

    MONGO_HOST=127.0.0.1 MONGO_PORT=27017 python -m pytest tests/test_live_mongo.py -v

Connection settings come from the environment, and the defaults match what the
other submissions configure: `localhost:27017`, database `financial_db`, no
authentication. Override with `MONGO_URI` plus `MONGO_DB`, or with
`MONGO_HOST` / `MONGO_PORT` / `MONGO_USER` / `MONGO_PASSWORD` / `MONGO_DB`.
When the database is only reachable through the bastion the platform already
uses, `MONGO_SSH_TUNNEL=1` opens the same tunnel `utils/mongodb.py` opens,
reading the platform's own variable names (`host`, `user`, `password`,
`ssh_port`) plus `MONGO_REMOTE_HOST` and `MONGO_REMOTE_PORT` for the database
port.

### One collection name collides

The fundamentals submission writes EODHD indicators to `technical_indicators`
in a **wide** layout, one document per ticker-day, keyed on
`(ticker, timestamp_ms, interval)` with the source under `metadata.source`.
This pipeline writes a **long** layout, one document per indicator value, keyed
on `(ticker, timestamp_ms, indicator_name, interval, source)`, mirroring the
SQL tables.

The two cannot share a collection. Their key has no source in it, so an IBKR
value for a ticker-day would overwrite the EODHD one; and the two document
shapes would sit side by side in the same place.

So this pipeline's default is `ibkr_technical_indicators`, and pointing it at
the shared collection is an explicit choice rather than what a first run does by
accident:

```bash
MONGO_INDICATOR_COLLECTION=technical_indicators python scripts/load_to_mongo.py ...
```

Whether the platform wants one indicators collection, with a source in its key,
is the platform owner's decision. Every collection name is overridable:
`MONGO_PRICE_COLLECTION`, `MONGO_QUOTE_COLLECTION`,
`MONGO_INDICATOR_COLLECTION`, `MONGO_ALPHA_COLLECTION`. `price_data` keeps its
name deliberately: it is the platform's own contract and this feed is a gap
filler in it.

A note on volume. The derived datasets keep the long `(name, value)` layout the
SQL tables use, which is what lets a new indicator or alpha formula land
without a migration. It also means a full load is large: roughly 0.8M price
documents, 10.6M indicator documents and 6.5M alpha documents. A document per
`(ticker, day)` holding a subdocument of values would be about a fifteenth of
that; it is not what this writes, because the SQL contract is the one the
submission documents. Say so and it is a small change.

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
    quotes/retrieval_date=YYYY-MM-DD/*.jsonl
  processed/
    prices/symbol=SYMBOL/part-000.parquet
    news/symbol=SYMBOL/part-000.parquet
  output/
    prices/event_year=YYYY/symbol=SYMBOL/part-000.parquet
    news/event_date=YYYY-MM-DD/symbol=SYMBOL/part-000.parquet
    indicators/event_year=YYYY/symbol=SYMBOL/part-000.parquet
    alphas/event_year=YYYY/symbol=SYMBOL/part-000.parquet
    quotes/event_date=YYYY-MM-DD/symbol=SYMBOL/part-000.parquet
    indicators_live/event_year=YYYY/symbol=SYMBOL/part-000.parquet
    alphas_live/event_year=YYYY/symbol=SYMBOL/part-000.parquet
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
News anchors each run at the current time (`reqHistoricalNews` returns the
newest headlines at or before its anchor) and follows IB's `hasMore` flag
backwards page by page until the checkpoint time minus the configured minute
overlap is reached, so a burst larger than one page is not lost -- with one
second-precision edge: more articles sharing one exact second than
`news.limit` holds cannot all be seen, and the run logs
`news_same_second_burst_may_skip_articles`. Each run's
walk is bounded by `news.max_pages`; a backlog beyond `limit x max_pages`
holds the checkpoint in place, logs `news_backlog_exceeds_page_budget`, and
needs that budget raised to cover it. Partition
writes then deduplicate prices by `(symbol, event_date)` and news by
`(symbol, provider_code, article_id)`.

## Recovery

If an Airflow worker dies between collecting and database-loading (parquet
advanced, `price_data` did not), the next run's watermark no longer covers the
stranded bars. Set `IBKR_PRICE_LOAD_FULL=true` for one run of `ibkr_prices` to
push the whole panel; the platform's DO NOTHING conflict policy absorbs every
row already present.

`stream-quotes` is a batch capture and holds every tick in memory until it
returns; long unbounded runs (`duration_seconds: 0`) belong to
`stream-pipeline`, which flushes to parquet at the flush interval.

## Validation evidence

Delivered dataset integrity, live-Gateway verification, measured latencies and the
known data characteristics (zero-volume sessions, short histories, stale or
recycled tickers, the `FISV`/`FI` rename) are recorded in
[`VALIDATION_EVIDENCE.md`](VALIDATION_EVIDENCE.md).

## Testing

Install development dependencies and run the Python 3.8 suite:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m compileall -q pipeline tests
```

`python scripts/verify_environment.py` validates a PLATFORM worker environment
(Airflow 2.8.1, PySpark 3.5.1, Java 17, and the platform's exact pins). Run it
on the worker image; in a local test venv it rightly reports the
platform-only packages as missing.

The Spark compatibility test starts a local PySpark 3.5.1 session and reads the
generated final Parquet partitions. Java 17 must be active for that test. It is
in `requirements-dev.txt` rather than `requirements.txt`, because the pipeline
writes Parquet with pyarrow and never imports Spark; without it that one test
skips and the rest of the suite runs.

`python scripts/write_dataset_readme.py --data-root <root>` prints a note
describing a collected dataset folder, with every count, date span and column
list read from the Parquet itself. `--out` writes it beside the data.

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
