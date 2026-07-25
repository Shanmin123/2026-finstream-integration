"""Derived feature stages: technical indicators and an Alpha101 subset.

Both stages read the pipeline's own final prices dataset and are pure pandas,
pinned to the production contract (Python 3.8, pandas 1.3.x). The full Alpha101
set (external ``alpha101`` package, Python 3.13 environment) and Qlib Alpha158
remain in the research repository; this module ships the contract-compatible
subset with each formula documented inline.

No look-ahead: every value at row t uses only rows <= t of the same input
(rolling / shift / expanding), except the explicitly cross-sectional
``alpha_20`` whose rank is taken across symbols within the same date.
"""

import numpy as np
import pandas as pd

PRICE_INPUT_COLUMNS = ("symbol", "event_date", "open", "high", "low", "close", "volume")

INDICATOR_COLUMNS = (
    "symbol",
    "event_date",
    "sma_20",
    "sma_50",
    "ema_12",
    "ema_26",
    "macd",
    "macd_signal",
    "macd_hist",
    "rsi_14",
    "bb_mid_20",
    "bb_upper_20",
    "bb_lower_20",
    "atr_14",
    "obv",
    "computed_at_utc",
)

ALPHA_COLUMNS = (
    "symbol",
    "event_date",
    "alpha_6",
    "alpha_9",
    "alpha_12",
    "alpha_20",
    "alpha_23",
    "alpha_53",
    "alpha_54",
    "alpha_101",
    "computed_at_utc",
)


def _prepare(frame):
    out = frame.copy()
    for column in ("open", "high", "low", "close", "volume"):
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out.sort_values(["symbol", "event_date"]).reset_index(drop=True)
    return out


def _wilder_smooth(series, period):
    """Wilder's smoothing: seed with the simple mean of the first ``period``
    observations, then avg_t = (avg_{t-1} * (period - 1) + x_t) / period.

    pandas' ewm(adjust=False) seeds from the first observation instead, which
    leaves a materially different level for the first weeks (RSI differed by
    ~17 points on an alternating series). Wilder's seeding is what RSI(14) and
    ATR(14) are defined as and what TA libraries report.
    """
    values = series.to_numpy(dtype="float64")
    out = np.full(len(values), np.nan)
    valid = np.flatnonzero(~np.isnan(values))
    if len(valid) < period:
        return pd.Series(out, index=series.index)
    seed_at = valid[period - 1]
    average = values[valid[:period]].mean()
    out[seed_at] = average
    for i in range(seed_at + 1, len(values)):
        x = values[i]
        if np.isnan(x):
            # No observation for this row: the smoothed value is unknown, not
            # "the same as yesterday". The running average is left untouched so
            # the series resumes correctly at the next real observation.
            continue
        average = (average * (period - 1) + x) / period
        out[i] = average
    return pd.Series(out, index=series.index)


def _delta(series, period):
    return series.diff(period)


def _delay(series, period):
    return series.shift(period)


def compute_indicators(prices, computed_at_utc):
    """Per-symbol technical indicators over daily OHLCV.

    RSI(14) Wilder smoothing, SMA(20/50), EMA(12/26), MACD(12,26,9),
    Bollinger(20, 2 std), ATR(14) Wilder, and OBV.
    """
    data = _prepare(prices)
    parts = []
    for symbol, g in data.groupby("symbol", sort=True):
        g = g.reset_index(drop=True)
        close = g["close"]
        high = g["high"]
        low = g["low"]
        volume = g["volume"]

        out = pd.DataFrame({"symbol": symbol, "event_date": g["event_date"]})
        out["sma_20"] = close.rolling(20).mean()
        out["sma_50"] = close.rolling(50).mean()
        ema_12 = close.ewm(span=12, adjust=False).mean()
        ema_26 = close.ewm(span=26, adjust=False).mean()
        out["ema_12"] = ema_12
        out["ema_26"] = ema_26
        macd = ema_12 - ema_26
        out["macd"] = macd
        out["macd_signal"] = macd.ewm(span=9, adjust=False).mean()
        out["macd_hist"] = macd - out["macd_signal"]

        change = close.diff()
        gain = change.clip(lower=0.0)
        loss = (-change).clip(lower=0.0)
        avg_gain = _wilder_smooth(gain, 14)
        avg_loss = _wilder_smooth(loss, 14)
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        out["rsi_14"] = rsi.where(avg_loss > 0.0, 100.0).where(
            ~(avg_gain + avg_loss).isna(), np.nan
        )

        mid = close.rolling(20).mean()
        std = close.rolling(20).std(ddof=0)
        out["bb_mid_20"] = mid
        out["bb_upper_20"] = mid + 2.0 * std
        out["bb_lower_20"] = mid - 2.0 * std

        prev_close = close.shift(1)
        # True range needs the previous close, so the first bar has none. max()
        # skips NaN by default and would return high-low there, seeding ATR a
        # row early off a bar that is not a true range.
        true_range = pd.concat(
            [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
            axis=1,
        ).max(axis=1).where(prev_close.notna())
        out["atr_14"] = _wilder_smooth(true_range, 14)

        direction = np.sign(change.fillna(0.0))
        out["obv"] = (direction * volume.fillna(0.0)).cumsum()

        out["computed_at_utc"] = computed_at_utc
        parts.append(out)
    if not parts:
        return pd.DataFrame(columns=list(INDICATOR_COLUMNS))
    result = pd.concat(parts, ignore_index=True)
    return result.loc[:, list(INDICATOR_COLUMNS)]


def compute_alphas(prices, computed_at_utc):
    """Alpha101 subset (Kakushadze 2015), pandas-only.

    Time-series alphas are computed per symbol; ``alpha_20`` ranks
    cross-sectionally across the symbols present on each date (with a single
    symbol each pct rank is 1.0, so alpha_20 degenerates to the constant -1;
    documented in schema.md).
    """
    data = _prepare(prices)
    parts = []
    for symbol, g in data.groupby("symbol", sort=True):
        g = g.reset_index(drop=True)
        open_ = g["open"]
        high = g["high"]
        low = g["low"]
        close = g["close"]
        volume = g["volume"]

        out = pd.DataFrame({"symbol": symbol, "event_date": g["event_date"]})

        # alpha_6: (-1 * correlation(open, volume, 10))
        out["alpha_6"] = -1.0 * open_.rolling(10).corr(volume)

        # alpha_9: if 0 < ts_min(delta(close,1),5) then delta(close,1)
        #          elif ts_max(delta(close,1),5) < 0 then delta(close,1)
        #          else -1*delta(close,1)
        d1 = _delta(close, 1)
        window_min = d1.rolling(5).min()
        window_max = d1.rolling(5).max()
        keep = (window_min > 0) | (window_max < 0)
        # Before five deltas exist both comparisons are False on NaN, which would
        # emit the -delta branch as if the condition had been evaluated.
        out["alpha_9"] = d1.where(keep, -1.0 * d1).where(window_min.notna())

        # alpha_12: sign(delta(volume,1)) * (-1 * delta(close,1))
        out["alpha_12"] = np.sign(_delta(volume, 1)) * (-1.0 * d1)

        # alpha_23: if (sum(high,20)/20) < high then -1*delta(high,2) else 0
        cond = (high.rolling(20).mean() < high)
        out["alpha_23"] = (-1.0 * _delta(high, 2)).where(cond, 0.0).where(
            high.rolling(20).mean().notna(), np.nan
        )

        # alpha_53: -1 * delta(((close-low)-(high-close))/(close-low), 9)
        # close == low (a session that closed on its low) makes the denominator
        # zero; the ratio is undefined there, not infinite.
        close_low = (close - low).replace(0.0, np.nan)
        inner = ((close - low) - (high - close)) / close_low
        out["alpha_53"] = -1.0 * _delta(inner, 9)

        # alpha_54: (-1*((low-close)*open^5)) / ((low-high)*close^5)
        # low == high (a zero-range session) is the same degenerate case.
        denom_54 = ((low - high) * close.pow(5)).replace(0.0, np.nan)
        out["alpha_54"] = (-1.0 * (low - close) * open_.pow(5)) / denom_54

        # alpha_101: (close-open) / ((high-low) + .001)
        out["alpha_101"] = (close - open_) / ((high - low) + 0.001)

        # alpha_20 inputs (cross-sectional rank applied after the concat below):
        # (-1*rank(open - delay(high,1))) * rank(open - delay(close,1))
        #   * rank(open - delay(low,1))
        out["_a20_h"] = open_ - _delay(high, 1)
        out["_a20_c"] = open_ - _delay(close, 1)
        out["_a20_l"] = open_ - _delay(low, 1)

        out["computed_at_utc"] = computed_at_utc
        parts.append(out)
    if not parts:
        return pd.DataFrame(columns=list(ALPHA_COLUMNS))
    result = pd.concat(parts, ignore_index=True)
    rank_h = result.groupby("event_date")["_a20_h"].rank(pct=True)
    rank_c = result.groupby("event_date")["_a20_c"].rank(pct=True)
    rank_l = result.groupby("event_date")["_a20_l"].rank(pct=True)
    result["alpha_20"] = (-1.0 * rank_h) * rank_c * rank_l
    result = result.drop(columns=["_a20_h", "_a20_c", "_a20_l"])
    # A factor is either a number or unknown: an infinity would propagate into
    # downstream models and into the database, where NaN is filtered but inf is
    # not. Any residual non-finite value becomes NaN here.
    value_columns = [c for c in ALPHA_COLUMNS if c.startswith("alpha_")]
    result[value_columns] = result[value_columns].replace([np.inf, -np.inf], np.nan)
    return result.loc[:, list(ALPHA_COLUMNS)]


LIVE_EXTRA_COLUMNS = ("as_of_utc", "provisional")


def compute_live_features(prices, bars_by_symbol, as_of_utc):
    """Provisional intraday refresh of the daily indicators and alphas.

    ``bars_by_symbol`` maps a symbol to the partial bar the stream has observed
    so far: ``{"event_date": "YYYY-MM-DD", "close": float}`` plus whichever of
    ``open``/``high``/``low``/``volume`` have ticked. Fields the stream has not
    reported stay NaN rather than being filled with the last price: a fabricated
    open/high/low would silently turn range-based formulas into constants
    (alpha_101 collapses to 0, alpha_53/54 divide by zero). With NaN the affected
    values come out NaN, which is the honest "not yet known".

    Returns only the provisional last row per symbol, tagged ``provisional``.
    Values converge to the final ones once the real daily bar replaces them.
    """
    data = _prepare(prices)
    extended = []
    for symbol, g in data.groupby("symbol", sort=True):
        bar = bars_by_symbol.get(symbol)
        if not bar or bar.get("close") is None:
            continue
        tick_date = str(bar["event_date"])
        g = g[g["event_date"] < tick_date]
        if g.empty:
            continue
        provisional = {
            "symbol": symbol,
            "event_date": tick_date,
            "open": bar.get("open", np.nan),
            "high": bar.get("high", np.nan),
            "low": bar.get("low", np.nan),
            "close": float(bar["close"]),
            "volume": bar.get("volume", np.nan),
        }
        extended.append(pd.concat([g, pd.DataFrame([provisional])], ignore_index=True))
    if not extended:
        empty_i = pd.DataFrame(columns=list(INDICATOR_COLUMNS) + list(LIVE_EXTRA_COLUMNS))
        empty_a = pd.DataFrame(columns=list(ALPHA_COLUMNS) + list(LIVE_EXTRA_COLUMNS))
        return empty_i, empty_a
    panel = pd.concat(extended, ignore_index=True)
    indicators = compute_indicators(panel, as_of_utc)
    alphas = compute_alphas(panel, as_of_utc)
    live_i = indicators.sort_values("event_date").groupby("symbol", sort=True).tail(1)
    live_a = alphas.sort_values("event_date").groupby("symbol", sort=True).tail(1)
    live_i = live_i.copy()
    live_a = live_a.copy()
    # Indicators that read fields the stream has not reported would otherwise
    # carry yesterday's value forward (Wilder smoothing) or treat a missing
    # volume as zero (OBV). Blank them for this row: not yet known.
    needs = {"atr_14": ("high", "low"), "obv": ("volume",)}
    for symbol_index, symbol in enumerate(live_i["symbol"]):
        observed = bars_by_symbol.get(symbol, {})
        for column, required in needs.items():
            if any(
                observed.get(field) is None
                or observed.get(field) != observed.get(field)
                for field in required
            ):
                live_i.iloc[symbol_index, live_i.columns.get_loc(column)] = np.nan
    for frame in (live_i, live_a):
        frame["as_of_utc"] = as_of_utc
        frame["provisional"] = True
    return live_i.reset_index(drop=True), live_a.reset_index(drop=True)
