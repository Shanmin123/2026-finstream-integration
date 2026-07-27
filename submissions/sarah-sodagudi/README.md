# Sarah Sodagudi — Fundamentals + Technical Indicators Pipelines

Two independent pipelines for this dissertation contribution to FinStreamAI:

1. **[Fundamentals Pipeline](#fundamentals-pipeline)** (`pipeline/`) — EODHD Fundamental Data API → MongoDB.
2. **[Technical Indicators Pipeline](#technical-indicators-pipeline)** (`pipeline/indicators/`) — the platform's own `price_data` → Spark/pandas computation → EODHD Technical Indicator API cross-check (a validated subset only) → MongoDB.

Each is self-contained (its own config/env files, its own Docker image, its own Airflow DAG) and can be run independently of the other.

---

## Fundamentals Pipeline

### Overview
Fetches annual and quarterly fundamentals for every active S&P 500 constituent from the EODHD Fundamental Data API and writes them to MongoDB. Captures every numeric field EODHD reports per fiscal period (Income Statement, Balance Sheet, Cash Flow, EPS) rather than a fixed taxonomy, so nothing is silently discarded. Ticker universe comes from the platform's shared `companies` table (falls back to a local CSV -sp500_union_constituents.csv if that's unreachable).

### Setup
```bash
# macOS / Linux
python -m venv venv
source venv/bin/activate
pip install -r pipeline/requirements.txt
cp pipeline/.env.example pipeline/.env
cp pipeline/config.example.yaml pipeline/config.yaml
# fill in pipeline/.env with your EODHD API token + Postgres user/password (secrets)
# fill in pipeline/config.yaml with hosts/ports/paths/params (non-secret)
# neither file is committed
```

```powershell
# Windows (PowerShell)
python -m venv venv
venv\Scripts\Activate.ps1
# if this errors with an execution-policy message, run once:
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
pip install -r pipeline/requirements.txt
copy pipeline\.env.example pipeline\.env
copy pipeline\config.example.yaml pipeline\config.yaml
# fill in pipeline/.env with your EODHD API token + Postgres user/password (secrets)
# fill in pipeline/config.yaml with hosts/ports/paths/params (non-secret)
# neither file is committed
```

### Running the pipeline

Two equivalent options:

#### Option A: directly
```bash
cd pipeline
python main.py
```

#### Option B: as an Airflow DAG
```bash
docker compose up -d --build sarah_fundamentals_airflow
docker compose ps   # wait for sarah_fundamentals_airflow to show "healthy"

docker exec sarah_fundamentals_airflow airflow dags unpause sarah_fundamentals_pipeline
docker exec sarah_fundamentals_airflow airflow dags trigger sarah_fundamentals_pipeline
```
Airflow UI at `http://localhost:8082` (admin/admin). The container mounts `pipeline/` (including your own `config.yaml`/`.env`, once you've created them per Setup above) straight into the DAG's execution environment.

For a cheap first test before running the full ticker universe (each ticker costs 10 real EODHD API requests — see `metadata.yaml`), set `run.ticker_limit` in `config.yaml` to a small number like `3`, for either option.

### Resuming after interruption
Two layers of resumability:
- **Within a run**: a JSON checkpoint (`paths.checkpoint_file` in config, default `./data/.checkpoint.json`) records which tickers have already been fetched, saved after *each* ticker rather than only at the end.
- **Across runs / at the data layer**: every MongoDB write is an upsert keyed on `(ticker, year, report_type)` for annual records and `(ticker, year, quarter, report_type)` for quarterly records.

The checkpoint resets automatically after a fully successful run, since this is a periodic refresh pipeline, not a one-shot backfill.

### Output
MongoDB `financial_db` database:
- `annual_fundamental` — one document per (ticker, fiscal year)
- `quarter_fundamental` — one document per (ticker, fiscal year, quarter)

Document shape documented in `metadata.yaml` under the `fundamentals` pipeline's `output.schema`; a small illustrative sample is in `samples/sample_annual_fundamental.json`.

### Known limitations
- **Field names typed from EODHD's documentation, not exhaustively verified.** A few (basic vs. diluted EPS split, preferred-stock dividends specifically) have no clean 1:1 EODHD equivalent — flagged inline in `pipeline/fundamentals_mapper.py`'s docstring.
- **Historical depth exceeds the 2010–2024 comparison baseline** used by a prior WRDS/Compustat-based fundamentals effort on this project — kept at full depth by default since it costs nothing extra per API call.
- **Ticker universe depends on an external, shared table** (`companies`) being kept current elsewhere in the platform.

---

## Technical Indicators Pipeline

### Overview
Computes real-time technical indicators (SMA/EMA/RSI/MACD/Bollinger Bands/ATR/ADX/CCI/Stochastic/Williams %R/Donchian/ROC/OBV/CMF/VWAP) and WorldQuant Alpha101-style formulas (Alpha#2/3/6/9/12/28/41/54/101) from the platform's own live `price_data` — no mock/synthetic data. A validated subset is additionally cross-checked against EODHD's Technical Indicator API and stored **alongside** the local value (never replacing it) for data-quality review, in `<Column>_eodhd` fields.

Two local compute stages:
1. **PySpark** (`indicator_calculator.py`) — everything expressible as a rolling-window aggregate (SMA, Bollinger Bands, Momentum, RSI simple-average, MACD SMA-approximation, OBV, the Alpha101-style formulas).
2. **pandas** (`pandas_indicators.py`) — recursive/smoothed indicators Spark's windowed aggregates can't express (true EMA, true EMA-based MACD, Wilder RSI/ATR/ADX/DI, CCI, Williams %R, Stochastic, Donchian, ROC, historical volatility, Z-score, CMF, session VWAP).

Then an additive vendor cross-check (`technical_comparison.py`) for the functions `config.yaml`'s `vendor.functions` enables (default: only the functions confirmed to agree well with local computation, see Known limitations below).

### Setup
```bash
# macOS / Linux
python -m venv venv
source venv/bin/activate
pip install -r pipeline/indicators/requirements.txt
cp pipeline/indicators/.env.example pipeline/indicators/.env
cp pipeline/indicators/config.example.yaml pipeline/indicators/config.yaml
# fill in pipeline/indicators/.env with your EODHD API token + Postgres user/password
# fill in pipeline/indicators/config.yaml with hosts/ports/vendor scope
# neither file is committed
```
PySpark also needs a real JRE installed locally (Java 8/11/17) if you're running Option A directly rather than the Docker option below — `JAVA_HOME` must be set and reachable.

```powershell
# Windows (PowerShell)
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r pipeline/indicators/requirements.txt
copy pipeline\indicators\.env.example pipeline\indicators\.env
copy pipeline\indicators\config.example.yaml pipeline\indicators\config.yaml
```

### Running the pipeline

#### Option A: directly
```bash
cd pipeline/indicators
python main.py
```

#### Option B: as an Airflow DAG
```bash
docker compose up -d --build sarah_indicators_airflow
docker compose ps   # wait for sarah_indicators_airflow to show "healthy" (PySpark's JVM takes a little longer to start than the fundamentals container)

docker exec sarah_indicators_airflow airflow dags unpause sarah_indicators_pipeline
docker exec sarah_indicators_airflow airflow dags trigger sarah_indicators_pipeline
```
Airflow UI at `http://localhost:8083` (admin/admin).

For a cheap first test, set `vendor.ticker_limit` in `config.yaml` to a small number (e.g. 3) — local computation always runs for every active ticker regardless; only the vendor cross-check step is capped.

### Resuming after interruption
Resumability here is the **MongoDB watermark itself** (`MAX(timestamp_ms)` already stored per interval), not a separate checkpoint file — `price_data` doesn't change once written, so an interrupted run that never reached the final bulk write simply gets recomputed from scratch on the next invocation, at no risk of double-counting (the same watermark-filtered window gets upserted, not appended). See `mongo_writer.py`'s module docstring.

### Output
MongoDB `financial_db.technical_indicators` collection — one document per (ticker, timestamp_ms, interval). Every field ending in `_eodhd` is vendor-sourced; every other field is computed locally. `vendor_fields_available` on each document lists exactly which `_eodhd` fields that document has. Document shape documented in `metadata.yaml` under the `technical_indicators` pipeline's `output.schema`; a real sample (sanitized) is in `samples/sample_technical_indicator.json`.

### Known limitations
- **Vendor function scope is deliberately narrow, evidence-based**: `vendor.functions` defaults to `"sma,bbands,macd"` — confirmed (via a real-data diagnostic, 503 tickers/10,563 documents) to agree well with local computation (correlation 0.85–1.00). `rsi` showed weak agreement (correlation 0.58) and is excluded pending investigation — leading hypothesis is an insufficient-local-history warm-up effect for Wilder smoothing, not yet confirmed. Everything else is untested so far, not confirmed bad — widen this (or set to `"ALL"`) once validated the same way.
- **MACD has two local versions, by design**: `MACD_Line_SMA_APPROX`/etc. (Spark stage, an SMA-based approximation — real MACD needs recursive EMA state Spark's windowed aggregates can't express) and `MACD_Line`/etc. (pandas stage, the true EMA-based calculation, what the vendor cross-check actually compares against). Same pattern as `RSI_14` (simple) vs. `RSI_14_WILDER`.
- **Momentum_10/ROC_10 (and similar 10+-bar indicators) will be null for a ticker's first ~10 bars** — expected for any fixed-length rolling window on a short price history, not missing/broken data. Coverage climbs toward 100% as more `price_data` accumulates.
- **Alpha#1/#4/#5 from the published "101 Formulaic Alphas" deliberately not implemented** — they need Ts_ArgMax/Ts_Rank (not expressible as a single built-in Spark window aggregate without an unverified UDF) or a VWAP series (only meaningful intraday in this pipeline).

---

## Reproducing the data quality reports referenced above
The vendor-agreement diagnostic and coverage-statistics scripts referenced in `metadata.yaml`/PR notes live in the wider integration tree's `tests/sarah/` folder (`diagnose_vendor_vs_local_indicators.py`, `summarize_technical_indicators_coverage.py`, `summarize_fundamentals_coverage.py`), not in this submission folder, since they read from the shared MongoDB instance rather than being part of either pipeline itself.
