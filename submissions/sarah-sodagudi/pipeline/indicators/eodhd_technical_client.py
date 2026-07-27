"""
EODHD Technical Indicator API client -- pulls every enabled vendor function.
Author: Sarah Sodagudi

Data source   : EODHD Technical Indicator API.
Endpoint      : GET {api.base_url}/{TICKER}.{EXCHANGE}
                ?function={sma|ema|rsi|macd|bbands|...}&period=...&api_token=...
Auth method   : API token as the `api_token` query parameter
                (config["api"]["api_key"], loaded from .env -- never
                hardcoded, see .env.example).
Rate limits   : Shared paid-plan credit budget; confirmed empirically as
                5 requests/call (not the usual 1) -- retried on HTTP 429
                with exponential backoff.
Output schema : Dict[function_key, list[dict]] -- one raw EODHD record
                list per function, field names exactly as EODHD returns
                them (NOT renamed/mapped onto local column names -- see
                technical_comparison.py for how these get merged in).

IMPORTANT LIMITATIONS:
1. EODHD's Technical Indicator API operates on EOD daily series only --
   no intraday granularity. Only meaningful for interval="1d".
2. Verified against real responses during development: every function
   below returned real data EXCEPT "mfi" and "williams_r", which both
   came back with 0 records -- either the function name is wrong or
   that function isn't available on the account's plan. Both are
   excluded from VENDOR_FUNCTIONS (moved to BROKEN_FUNCTIONS,
   documentation-only, never called) rather than silently burning
   credits on an endpoint that returns nothing every run.

Credit-cost note: pulls every function in `config["vendor"]["functions"]`
for up to `config["vendor"]["ticker_limit"]` tickers on every 1d run --
see config.example.yaml for the current default (the functions confirmed,
via diagnose_vendor_vs_local_indicators.py against real data, to agree
well with the local calculation) and README.md's cost table before
widening either setting. Tokens match on the underlying function name (so
"sma" enables SMA_10/20/50 together), not just the exact period-suffixed
key -- see enabled_vendor_function_keys()'s docstring.

Date-range note: every fetch is bounded to the trailing
config["vendor"]["lookback_days"] via EODHD's `from`/`to` params. Without
this, EODHD returns a ticker's ENTIRE available history (40+ years of
daily rows for long-listed firms) on every call.
"""
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# key -> {"function": EODHD `function=` value, "period": int|None}
# `period` is omitted from the request entirely when None (lets EODHD use
# its own default for that function). technical_comparison.py merges
# whatever fields come back generically, so the inline field-name comments
# below are documentation, not something the merge code depends on.
VENDOR_FUNCTIONS = {
    "SMA_10": {"function": "sma", "period": 10},            # date, sma
    "SMA_20": {"function": "sma", "period": 20},             # date, sma
    "SMA_50": {"function": "sma", "period": 50},             # date, sma
    "EMA_12": {"function": "ema", "period": 12},             # date, ema
    "EMA_26": {"function": "ema", "period": 26},             # date, ema
    "WMA_20": {"function": "wma", "period": 20},             # date, wma
    "RSI_14": {"function": "rsi", "period": 14},             # date, rsi
    "STDDEV_20": {"function": "stddev", "period": 20},       # date, stddev
    "VOLATILITY_20": {"function": "volatility", "period": 20},  # date, volatility
    "MACD": {"function": "macd", "period": None},            # date, macd, signal, divergence (fixed 12/26/9, period param has no effect)
    "BBANDS_20": {"function": "bbands", "period": 20},       # date, uband, mband, lband
    "ATR_14": {"function": "atr", "period": 14},             # date, atr
    "ADX_14": {"function": "adx", "period": 14},             # date, adx
    "DMI_14": {"function": "dmi", "period": 14},             # date, dmi (single value, not split +DI/-DI)
    "CCI_20": {"function": "cci", "period": 20},             # date, cci
    "STOCHASTIC": {"function": "stochastic", "period": None},  # date, k_values, d_values
    "STOCHRSI_14": {"function": "stochrsi", "period": 14},   # date, fastkline, fastdline
    "ROC_10": {"function": "roc", "period": 10},             # date, roc
    "SAR": {"function": "sar", "period": None},              # date, sar
    "SLOPE_10": {"function": "slope", "period": 10},         # date, slope
}

# Confirmed BROKEN against a live response (0 records returned for both)
# -- excluded from VENDOR_FUNCTIONS so default runs don't waste credits
# calling them every time. Either the function name is wrong or these
# aren't available on the account's plan; needs checking against EODHD
# support/docs directly before re-enabling.
BROKEN_FUNCTIONS = {
    "MFI_14": {"function": "mfi", "period": 14},
    "WILLIAMS_R_14": {"function": "williams_r", "period": 14},
}


def enabled_vendor_function_keys(config: dict) -> List[str]:
    """
    Which VENDOR_FUNCTIONS keys are active, per config["vendor"]["functions"]
    ("ALL" or a comma-separated subset).

    Each requested token matches in TWO ways (case-insensitive):
      1. Exact key match, e.g. "RSI_14", "BBANDS_20", "MACD".
      2. Base-function match, e.g. "sma" enables every SMA_10/SMA_20/
         SMA_50 key at once, "rsi" enables RSI_14, "bbands" enables
         BBANDS_20 -- matches VENDOR_FUNCTIONS[key]["function"] rather
         than requiring the exact period-suffixed key name. This is the
         forgiving/expected form -- e.g. functions: "sma,rsi,macd,bbands"
         enables SMA_10+SMA_20+SMA_50, RSI_14, MACD, and BBANDS_20, not
         just the one key (MACD) that happens to have no period suffix.
    """
    raw = str(config["vendor"]["functions"]).strip()
    if raw.upper() == "ALL":
        return list(VENDOR_FUNCTIONS.keys())

    requested = {tok.strip().upper() for tok in raw.split(",") if tok.strip()}
    base_function_names = {spec["function"].upper() for spec in VENDOR_FUNCTIONS.values()}
    unknown = requested - set(VENDOR_FUNCTIONS.keys()) - base_function_names
    if unknown:
        logger.warning("vendor.functions has unrecognised keys, ignoring: %s", unknown)

    return [
        key for key, spec in VENDOR_FUNCTIONS.items()
        if key in requested or spec["function"].upper() in requested
    ]


def _get_with_retry(url: str, params: Dict, max_retries: int, backoff_seconds: float, timeout_seconds: int) -> Optional[requests.Response]:
    """GET `url` with `params`, retrying on HTTP 429/5xx with exponential
    backoff. Returns the Response on success, or None if every attempt failed."""
    backoff = backoff_seconds
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout_seconds)
        except requests.RequestException as exc:
            logger.warning("Request error (attempt %d/%d) for %s: %s", attempt, max_retries, url, exc)
            time.sleep(backoff)
            backoff *= 2
            continue

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after else backoff
            logger.warning("Rate-limited (429) on attempt %d/%d for %s; waiting %.1fs", attempt, max_retries, url, wait_s)
            time.sleep(wait_s)
            backoff *= 2
            continue

        if resp.status_code >= 500:
            logger.warning("Server error %d (attempt %d/%d) for %s", resp.status_code, attempt, max_retries, url)
            time.sleep(backoff)
            backoff *= 2
            continue

        return resp

    logger.error("Request permanently failed after %d attempts for %s", max_retries, url)
    return None


def fetch_technical_indicator(
    config: dict, ticker: str, exchange: str, function: str, period: Optional[int] = None,
    from_date: Optional[str] = None, to_date: Optional[str] = None,
) -> List[Dict]:
    """
    Fetch one EODHD technical-indicator series for a ticker, bounded to
    [from_date, to_date] (both "YYYY-MM-DD") when given. Returns [] on any
    failure (no data, HTTP error, retries exhausted, unrecognised function
    name) -- never raises, so one bad/unverified function can't take down
    the whole run.
    """
    api_cfg = config["api"]
    if not api_cfg.get("api_key") or api_cfg["api_key"] == "YOUR_EODHD_API_TOKEN_HERE":
        logger.error("api.api_key is not set -- cannot fetch technical indicators.")
        return []

    url = f"{api_cfg['base_url']}/{ticker}.{exchange}"
    params = {"api_token": api_cfg["api_key"], "fmt": "json", "function": function}
    if period is not None:
        params["period"] = period
    if from_date is not None:
        params["from"] = from_date
    if to_date is not None:
        params["to"] = to_date

    resp = _get_with_retry(
        url, params,
        max_retries=api_cfg.get("max_retries", 5),
        backoff_seconds=api_cfg.get("backoff_seconds", 2),
        timeout_seconds=api_cfg.get("http_timeout_seconds", 30),
    )
    if resp is None or resp.status_code != 200:
        status = resp.status_code if resp is not None else "no response"
        logger.warning("Vendor technical indicator fetch failed for %s.%s function=%s: %s", ticker, exchange, function, status)
        return []

    try:
        data = resp.json()
    except ValueError:
        logger.warning("Vendor technical indicator response for %s.%s function=%s was not valid JSON.", ticker, exchange, function)
        return []

    if not isinstance(data, list):
        logger.warning("Unexpected vendor technical indicator response shape for %s.%s function=%s: %s", ticker, exchange, function, type(data))
        return []

    return data


def fetch_all_enabled_indicators(config: dict, ticker: str, exchange: str = "US") -> Dict[str, List[Dict]]:
    """
    Fetch every enabled VENDOR_FUNCTIONS series for one ticker, bounded to
    the trailing config["vendor"]["lookback_days"]. Returns
    {vendor_key: [raw EODHD records]}, keys with no data omitted.
    """
    lookback_days = config["vendor"]["lookback_days"]
    to_date = datetime.now(timezone.utc).date()
    from_date = to_date - timedelta(days=lookback_days)

    results = {}
    for key in enabled_vendor_function_keys(config):
        spec = VENDOR_FUNCTIONS[key]
        records = fetch_technical_indicator(
            config, ticker, exchange, spec["function"], spec.get("period"),
            from_date=from_date.isoformat(), to_date=to_date.isoformat(),
        )
        if records:
            results[key] = records
        else:
            logger.warning("No data returned for vendor function %s (%s) for %s.%s.", key, spec["function"], ticker, exchange)
    return results
