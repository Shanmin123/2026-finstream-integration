"""
EODHD vendor indicator cross-check merge.
Author: Sarah Sodagudi

Runs AFTER indicator_calculator.py's PySpark computation and AFTER
`spark_df.toPandas()`, since this issues per-ticker HTTP calls -- doing
that inside Spark executors would be awkward to rate-limit sensibly.

Purely ADDITIVE: locally-computed columns are NEVER modified or
overwritten. For every indicator EODHD's Technical Indicator API
genuinely covers AND is confirmed working (SMA/EMA/RSI-Wilder/MACD/
BBands/ATR/ADX/CCI/Stochastic/ROC -- see OVERLAP_FIELD_MAP), the vendor
value is added as a NEW `<LocalColumn>_eodhd` column alongside the
existing local one, so both can be reviewed side by side in MongoDB and
cross-checked against each other (see tests/summarize_technical_indicators_coverage.py
in the wider repo for a full agreement-rate report). Indicators EODHD
offers with no local counterpart (STOCHRSI, WMA, SAR, Slope, StdDev,
Volatility) are also added as `_eodhd` columns, same as before.
Indicators with no vendor equivalent at all (Momentum, OBV, +DI/-DI
split, Donchian, Z-score, CMF, VWAP, every Alpha) or where the vendor
endpoint is confirmed broken (Williams %R) stay local-only, untouched.

Naming convention: every field ending in `_eodhd` came from EODHD's
Technical Indicator API; every other field was computed locally from
price_data. `vendor_fields_available` on each document is the complete,
literal list of that document's own `_eodhd` field names (covering both
the OVERLAP_FIELD_MAP replacements-as-additions and the vendor-exclusive
fields) -- e.g. `for f in doc["vendor_fields_available"]: doc[f]` just
works, no lookup table needed.

Gated by config["vendor"]["functions"] (which functions) and
config["vendor"]["ticker_limit"] (how many distinct tickers get a
vendor fetch per run, since this costs real EODHD credits on top of the
free local computation -- local computation/storage for every ticker is
unaffected by this limit, it only controls which tickers ALSO get vendor
fields).

Only meaningful for interval="1d": EODHD's Technical Indicator API is
EOD-daily only, so 5m intraday runs skip this entirely (logged, not
silently dropped).
"""
import logging

import numpy as np
import pandas as pd

from eodhd_technical_client import enabled_vendor_function_keys, fetch_all_enabled_indicators

logger = logging.getLogger(__name__)

# canonical local column -> (vendor function key, vendor response field).
# Only indicators EODHD confirmedly returns real data for (see
# eodhd_technical_client.py's VENDOR_FUNCTIONS / BROKEN_FUNCTIONS) belong
# here. RSI maps onto RSI_14_WILDER specifically, not the simple-average
# RSI_14 -- EODHD's "rsi" function is Wilder-smoothed, the standard
# definition, same as our pandas-stage Wilder RSI, not our Spark-stage
# simple-average approximation (which stays a distinct, intentionally
# different metric -- not compared against vendor under this mapping).
OVERLAP_FIELD_MAP = {
    "SMA_10": ("SMA_10", "sma"),
    "SMA_20": ("SMA_20", "sma"),
    "SMA_50": ("SMA_50", "sma"),
    "EMA_12": ("EMA_12", "ema"),
    "EMA_26": ("EMA_26", "ema"),
    "RSI_14_WILDER": ("RSI_14", "rsi"),
    "MACD_Line": ("MACD", "macd"),
    "MACD_Signal": ("MACD", "signal"),
    "MACD_Histogram": ("MACD", "divergence"),
    "BB_Upper": ("BBANDS_20", "uband"),
    "BB_Lower": ("BBANDS_20", "lband"),
    "ATR_14": ("ATR_14", "atr"),
    "ADX_14": ("ADX_14", "adx"),
    "CCI_20": ("CCI_20", "cci"),
    "STOCH_K_14": ("STOCHASTIC", "k_values"),
    "STOCH_D_3": ("STOCHASTIC", "d_values"),
    "ROC_10": ("ROC_10", "roc"),
}


def _to_number(raw):
    if raw is None:
        return np.nan
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped or stripped.lower() == "none":
            return np.nan
        try:
            return float(stripped)
        except ValueError:
            return np.nan
    return np.nan


def _build_ticker_vendor_frame(config: dict, ticker: str, exchange: str) -> pd.DataFrame:
    """One row per vendor-reported date for `ticker`, columns from every
    enabled vendor function, outer-joined on date (functions can have
    different available date ranges). Raw field names, un-renamed --
    enrich_with_vendor_indicators does the renaming."""
    vendor_data = fetch_all_enabled_indicators(config, ticker, exchange)
    if not vendor_data:
        return pd.DataFrame()

    per_function_frames = []
    for key, records in vendor_data.items():
        vdf = pd.DataFrame(records)
        if "date" not in vdf.columns:
            continue
        vdf["date"] = pd.to_datetime(vdf["date"])
        value_cols = [c for c in vdf.columns if c != "date"]
        if not value_cols:
            continue
        for c in value_cols:
            vdf[c] = vdf[c].apply(_to_number)
        vdf = vdf[["date"] + value_cols].rename(columns={c: f"{key}__{c}" for c in value_cols})
        per_function_frames.append(vdf.set_index("date"))

    if not per_function_frames:
        return pd.DataFrame()

    merged = pd.concat(per_function_frames, axis=1, join="outer").reset_index()
    merged["ticker"] = ticker
    return merged


def enrich_with_vendor_indicators(config: dict, pdf: pd.DataFrame, interval: str, exchange: str = "US") -> pd.DataFrame:
    """
    Returns `pdf` unchanged if it's empty, if no vendor functions are
    enabled, or if `interval` isn't "1d". Otherwise returns a copy with:
      - one `<LocalColumn>_eodhd` column per OVERLAP_FIELD_MAP entry the
        vendor had data for -- the ORIGINAL local column is never
        modified, so both are available side by side for cross-checking;
      - one `<VendorKey>__<field>_eodhd` column per vendor-exclusive
        field (no local counterpart);
      - a `vendor_fields_available` column: the complete, literal list of
        every `_eodhd` column name present on that row (both kinds
        above), so a reader never has to guess which vendor fields a
        given document actually has.
    If config["vendor"]["ticker_limit"] is set, only that many distinct
    tickers get a vendor fetch (cost control) -- every ticker still keeps
    its full local computation regardless.
    """
    if pdf.empty:
        return pdf

    if interval != "1d":
        logger.info("Skipping EODHD vendor indicator fetch for interval=%s: the Technical Indicator API is EOD-daily only.", interval)
        return pdf

    enabled_keys = enabled_vendor_function_keys(config)
    if not enabled_keys:
        return pdf

    result = pdf.copy()
    result["datetime_utc"] = pd.to_datetime(result["datetime_utc"])
    result["vendor_fields_available"] = [[] for _ in range(len(result))]

    ticker_limit = config["vendor"].get("ticker_limit") or None
    all_tickers = list(result["ticker"].unique())
    vendor_tickers = all_tickers[:ticker_limit] if ticker_limit else all_tickers
    if ticker_limit and len(all_tickers) > len(vendor_tickers):
        logger.warning(
            "vendor.ticker_limit=%d set -- vendor-fetching only %d of %d tickers this run "
            "(local values unaffected for the rest).", ticker_limit, len(vendor_tickers), len(all_tickers),
        )

    vendor_frames = [_build_ticker_vendor_frame(config, t, exchange) for t in vendor_tickers]
    vendor_frames = [f for f in vendor_frames if not f.empty]

    if not vendor_frames:
        logger.warning("No vendor indicator data retrieved for any ticker; local values are all that's available this run.")
        return result

    vendor_df = pd.concat(vendor_frames, ignore_index=True, sort=False)

    merged = pd.merge_asof(
        result.sort_values("datetime_utc"),
        vendor_df.sort_values("date"),
        left_on="datetime_utc",
        right_on="date",
        by="ticker",
        direction="backward",
        tolerance=pd.Timedelta("3D"),
    )
    if "date" in merged.columns:
        merged = merged.drop(columns=["date"])

    # --- Add vendor values as NEW *_eodhd columns; never touch the local ones ---
    for local_col, (vendor_key, vendor_field) in OVERLAP_FIELD_MAP.items():
        raw_col = f"{vendor_key}__{vendor_field}"
        if raw_col not in merged.columns:
            continue
        has_vendor_value = merged[raw_col].notna()
        if not has_vendor_value.any():
            merged = merged.drop(columns=[raw_col])
            continue
        eodhd_col = f"{local_col}_eodhd"
        merged[eodhd_col] = merged[raw_col]
        for idx in merged.index[has_vendor_value]:
            merged.at[idx, "vendor_fields_available"].append(eodhd_col)
        merged = merged.drop(columns=[raw_col])

    # --- Remaining raw__field columns are vendor-exclusive (no local
    # counterpart, e.g. STOCHRSI/WMA/SAR/Slope/StdDev/Volatility). Track
    # these in vendor_fields_available too, per row, the same way the
    # OVERLAP_FIELD_MAP columns above are tracked. ---
    leftover_raw_cols = [c for c in merged.columns if "__" in c and not c.endswith("_eodhd")]
    for raw_col in leftover_raw_cols:
        has_vendor_value = merged[raw_col].notna()
        eodhd_col = f"{raw_col}_eodhd"
        for idx in merged.index[has_vendor_value]:
            merged.at[idx, "vendor_fields_available"].append(eodhd_col)
    merged = merged.rename(columns={c: f"{c}_eodhd" for c in leftover_raw_cols})

    n_fields = sum(len(v) for v in merged["vendor_fields_available"])
    logger.info(
        "Vendor cross-check fields added for %d field-values across %d tickers; "
        "%d vendor-exclusive column(s) also added.", n_fields, len(vendor_frames), len(leftover_raw_cols),
    )

    return merged
