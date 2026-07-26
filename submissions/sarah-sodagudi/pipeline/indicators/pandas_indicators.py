"""
Pandas-stage indicators: recursive/smoothed computations.
Author: Sarah Sodagudi

indicator_calculator.py (PySpark) computes everything that's naturally a
rolling-window aggregate (SMA, rolling stddev, rolling min/max, rolling
correlation). True EMA and Wilder's smoothing (used by real RSI/ATR/ADX)
are recursive -- each value depends on the previous smoothed value, not a
fixed window -- which Spark's windowed aggregate functions can't express
directly. Rather than fake it with a wide rolling-window approximation,
this module runs the recursive versions in plain pandas (pandas==1.3.5
API only -- `.ewm(adjust=False)`, no 2.x-only arguments) AFTER
`spark_df.toPandas()`, the same "Spark for windows, pandas for recursive
smoothing" split already used by technical_comparison.py for the vendor
merge.

Note: RSI_14 (simple-average, computed in indicator_calculator.py) and
RSI_14_WILDER (this module) are DELIBERATELY both kept -- they're
different, textbook-standard smoothing choices for the same indicator,
useful to compare directly.

Performance note: CCI_20's mean-absolute-deviation term has no vectorised
pandas 1.3.5 rolling method, so it uses `.rolling().apply(..., raw=False)`
(a per-window Python callback) -- fine for a few hundred tickers, would
need a rewrite for very large universes.
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df.groupby("ticker")["close"].shift(1)
    ranges = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1)
    return ranges.max(axis=1)


def add_pandas_indicators(pdf: pd.DataFrame, interval: str) -> pd.DataFrame:
    """
    Adds, per ticker in time order:
        EMA_12, EMA_26, EMA_50           - true exponential moving averages
        MACD_Line, MACD_Signal,
        MACD_Histogram                   - true EMA-based MACD (12/26/9),
                                            the textbook definition and the
                                            one EODHD's vendor MACD is
                                            compared against. Not to be
                                            confused with indicator_calculator.py's
                                            MACD_*_SMA_APPROX, a native-Spark
                                            approximation kept only as a
                                            reference point.
        RSI_14_WILDER                    - Wilder-smoothed RSI
        ATR_14                           - Wilder-smoothed Average True Range
        PLUS_DI_14, MINUS_DI_14, ADX_14  - Directional Movement Index family
        CCI_20                           - Commodity Channel Index
        WILLIAMS_R_14                    - Williams %R
        STOCH_K_14, STOCH_D_3            - Stochastic oscillator
        DONCHIAN_UPPER_20, DONCHIAN_LOWER_20
        ROC_10                           - Rate of change (%)
        HIST_VOL_20                      - rolling stddev of log returns
        ZSCORE_20                        - (close - SMA_20) / rolling stddev
        CMF_20                           - Chaikin Money Flow
        VWAP_SESSION                     - session-cumulative VWAP.
                                            Only meaningful intraday: for
                                            interval="1d" the single daily
                                            bar already IS the session, so
                                            this is left null for 1d.
    """
    if pdf.empty:
        return pdf

    df = pdf.sort_values(["ticker", "datetime_utc"]).reset_index(drop=True)

    # --- True EMA ---
    for span, out_col in ((12, "EMA_12"), (26, "EMA_26"), (50, "EMA_50")):
        df[out_col] = df.groupby("ticker")["close"].transform(
            lambda s, span=span: s.ewm(span=span, adjust=False).mean()
        )

    # --- True MACD (EMA_12 - EMA_26, 9-period EMA signal) -- the textbook
    # definition, and what EODHD's vendor MACD is actually comparable
    # against (unlike indicator_calculator.py's SMA-based approximation) ---
    df["MACD_Line"] = df["EMA_12"] - df["EMA_26"]
    df["MACD_Signal"] = df.groupby("ticker")["MACD_Line"].transform(
        lambda s: s.ewm(span=9, adjust=False).mean()
    )
    df["MACD_Histogram"] = df["MACD_Line"] - df["MACD_Signal"]

    # --- Wilder-smoothed RSI_14 ---
    delta = df.groupby("ticker")["close"].diff()
    df["_gain"] = delta.clip(lower=0)
    df["_loss"] = (-delta).clip(lower=0)
    avg_gain = df.groupby("ticker")["_gain"].transform(lambda s: s.ewm(alpha=1 / 14, adjust=False).mean())
    avg_loss = df.groupby("ticker")["_loss"].transform(lambda s: s.ewm(alpha=1 / 14, adjust=False).mean())
    # Zero losses over the trailing window is a NORMAL outcome during a
    # sustained uptrend (not missing data) -- `.replace(0, np.nan)` would
    # wrongly NaN out RSI for the entire remainder of an uninterrupted
    # uptrend (avg_loss stays exactly 0 once there's been no loss at all
    # since the series start). An epsilon denominator instead correctly
    # pushes RSI to ~100 in that case, matching the simple-average RSI_14
    # in indicator_calculator.py, which already guards this the same way.
    rs = avg_gain / (avg_loss + 1e-9)
    df["RSI_14_WILDER"] = 100 - (100 / (1 + rs))

    # --- Wilder ATR_14 ---
    df["_true_range"] = _true_range(df)
    df["ATR_14"] = df.groupby("ticker")["_true_range"].transform(lambda s: s.ewm(alpha=1 / 14, adjust=False).mean())

    # --- Directional Movement Index family (ADX_14) ---
    up_move = df.groupby("ticker")["high"].diff()
    down_move = -df.groupby("ticker")["low"].diff()
    df["_plus_dm"] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    df["_minus_dm"] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    smoothed_plus_dm = df.groupby("ticker")["_plus_dm"].transform(lambda s: s.ewm(alpha=1 / 14, adjust=False).mean())
    smoothed_minus_dm = df.groupby("ticker")["_minus_dm"].transform(lambda s: s.ewm(alpha=1 / 14, adjust=False).mean())
    df["PLUS_DI_14"] = 100 * (smoothed_plus_dm / (df["ATR_14"] + 1e-9))
    df["MINUS_DI_14"] = 100 * (smoothed_minus_dm / (df["ATR_14"] + 1e-9))
    di_sum = df["PLUS_DI_14"] + df["MINUS_DI_14"]
    df["_dx"] = 100 * (df["PLUS_DI_14"] - df["MINUS_DI_14"]).abs() / (di_sum + 1e-9)
    df["ADX_14"] = df.groupby("ticker")["_dx"].transform(lambda s: s.ewm(alpha=1 / 14, adjust=False).mean())

    # --- CCI_20 ---
    df["_typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    tp_sma = df.groupby("ticker")["_typical_price"].transform(lambda s: s.rolling(20, min_periods=1).mean())
    mean_dev = df.groupby("ticker")["_typical_price"].transform(
        lambda s: s.rolling(20, min_periods=1).apply(lambda w: (w - w.mean()).abs().mean(), raw=False)
    )
    df["CCI_20"] = (df["_typical_price"] - tp_sma) / (0.015 * mean_dev + 1e-9)

    # --- Williams %R_14 / Stochastic %K_14 / %D_3 ---
    roll_high = df.groupby("ticker")["high"].transform(lambda s: s.rolling(14, min_periods=1).max())
    roll_low = df.groupby("ticker")["low"].transform(lambda s: s.rolling(14, min_periods=1).min())
    hl_range = roll_high - roll_low
    df["WILLIAMS_R_14"] = -100 * (roll_high - df["close"]) / (hl_range + 1e-9)
    df["STOCH_K_14"] = 100 * (df["close"] - roll_low) / (hl_range + 1e-9)
    df["STOCH_D_3"] = df.groupby("ticker")["STOCH_K_14"].transform(lambda s: s.rolling(3, min_periods=1).mean())

    # --- Donchian Channels (20-period) ---
    df["DONCHIAN_UPPER_20"] = df.groupby("ticker")["high"].transform(lambda s: s.rolling(20, min_periods=1).max())
    df["DONCHIAN_LOWER_20"] = df.groupby("ticker")["low"].transform(lambda s: s.rolling(20, min_periods=1).min())

    # --- Rate of Change (10-period, %) ---
    df["ROC_10"] = df.groupby("ticker")["close"].transform(lambda s: s.pct_change(periods=10) * 100)

    # --- Historical volatility (20-period rolling stddev of log returns) ---
    df["_log_ret"] = df.groupby("ticker")["close"].transform(lambda s: np.log(s / s.shift(1)))
    df["HIST_VOL_20"] = df.groupby("ticker")["_log_ret"].transform(lambda s: s.rolling(20, min_periods=2).std())

    # --- Z-score of close vs. its own 20-period mean/stddev ---
    close_sma20 = df.groupby("ticker")["close"].transform(lambda s: s.rolling(20, min_periods=1).mean())
    close_std20 = df.groupby("ticker")["close"].transform(lambda s: s.rolling(20, min_periods=1).std())
    df["ZSCORE_20"] = (df["close"] - close_sma20) / close_std20.replace(0, np.nan)

    # --- Chaikin Money Flow (20-period) ---
    money_flow_mult = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / (df["high"] - df["low"]).replace(0, np.nan)
    df["_mfv"] = money_flow_mult * df["volume"]
    mfv_sum = df.groupby("ticker")["_mfv"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    vol_sum = df.groupby("ticker")["volume"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    df["CMF_20"] = mfv_sum / vol_sum.replace(0, np.nan)

    # --- Session VWAP (intraday only) ---
    if interval == "5m":
        df["_session_date"] = df["datetime_utc"].dt.date
        df["_tp_vol"] = df["_typical_price"] * df["volume"]
        cum_tp_vol = df.groupby(["ticker", "_session_date"])["_tp_vol"].cumsum()
        cum_vol = df.groupby(["ticker", "_session_date"])["volume"].cumsum()
        df["VWAP_SESSION"] = cum_tp_vol / cum_vol.replace(0, np.nan)
    else:
        df["VWAP_SESSION"] = np.nan

    drop_cols = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=drop_cols)

    logger.info(f"Added pandas-stage indicators for {df['ticker'].nunique()} tickers, {len(df)} rows.")
    return df
