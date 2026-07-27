"""
PySpark Transformation Layer: Expanded Technical & Alpha 101 Indicators
Author: Sarah Sodagudi

Input        : a Spark DataFrame shaped like the shared price_data table
               (ticker, timestamp_ms, datetime_utc, open, high, low,
               close, volume, interval), with NULL-volume rows already
               filtered out by price_reader.py. Ordering uses
               `timestamp_ms` (bigint epoch ms, price_data's own
               primary-key component) rather than `datetime_utc`, since
               price_data has no plain `timestamp` column.
Output       : the same DataFrame with technical + Alpha 101-style
               indicator columns appended. Vendor (EODHD Technical
               Indicator API) comparison columns, where enabled, are
               merged in afterwards at the pandas level -- see
               technical_comparison.py -- not here, since this module
               stays pure-Spark/local-compute.
"""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

def calculate_technical_indicators(df: DataFrame) -> DataFrame:
    time_window = Window.partitionBy("ticker").orderBy("timestamp_ms")

    # Base computations for later indicators
    df = df.withColumn("price_diff", F.col("close") - F.lag("close", 1).over(time_window))

    # 1. Expanded Moving Averages
    df = df.withColumn("SMA_10", F.avg("close").over(time_window.rowsBetween(-9, Window.currentRow)))
    df = df.withColumn("SMA_20", F.avg("close").over(time_window.rowsBetween(-19, Window.currentRow)))
    df = df.withColumn("SMA_50", F.avg("close").over(time_window.rowsBetween(-49, Window.currentRow)))

    # 2. Bollinger Bands (+ width / %B, common derived readings)
    df = df.withColumn("stddev_20", F.stddev("close").over(time_window.rowsBetween(-19, Window.currentRow)))
    df = df.withColumn("BB_Upper", F.col("SMA_20") + (F.col("stddev_20") * 2))
    df = df.withColumn("BB_Lower", F.col("SMA_20") - (F.col("stddev_20") * 2))
    df = df.withColumn("BB_Width", (F.col("BB_Upper") - F.col("BB_Lower")) / F.col("SMA_20"))
    df = df.withColumn(
        "BB_PctB",
        (F.col("close") - F.col("BB_Lower")) / (F.col("BB_Upper") - F.col("BB_Lower") + 1e-9),
    )

    # 3. Expanded Momentum
    df = df.withColumn("Momentum_1", F.col("price_diff"))
    df = df.withColumn("Momentum_5", F.col("close") - F.lag("close", 5).over(time_window))
    df = df.withColumn("Momentum_10", F.col("close") - F.lag("close", 10).over(time_window))

    # 4. RSI (14-period)
    gain = F.when(F.col("price_diff") > 0, F.col("price_diff")).otherwise(0)
    loss = F.when(F.col("price_diff") < 0, -F.col("price_diff")).otherwise(0)
    df = df.withColumn("gain", gain).withColumn("loss", loss)

    window_14 = time_window.rowsBetween(-13, Window.currentRow)
    df = df.withColumn("avg_gain", F.avg("gain").over(window_14))
    df = df.withColumn("avg_loss", F.avg("loss").over(window_14))
    rs = F.col("avg_gain") / (F.col("avg_loss") + 1e-9)
    df = df.withColumn("RSI_14", 100 - (100 / (1 + rs)))

    # 5. MACD_*_SMA_APPROX -- SMA-based approximation, native PySpark windows
    # only (no recursive/EMA state across rows, which Spark's windowed
    # aggregates can't express). This is NOT the textbook MACD -- real MACD
    # is EMA(12)-EMA(26) with a 9-period EMA signal line, both recursive.
    # The true EMA-based MACD_Line/MACD_Signal/MACD_Histogram (the ones
    # OVERLAP_FIELD_MAP in technical_comparison.py actually cross-checks
    # against EODHD's vendor MACD) are computed in pandas_indicators.py
    # from its true EMA_12/EMA_26 columns instead. Kept here, distinctly
    # named, only as a reference point for how much the SMA approximation
    # diverges from the real thing -- same "keep both, name them honestly"
    # pattern as RSI_14 (simple) vs. RSI_14_WILDER.
    df = df.withColumn("SMA_12", F.avg("close").over(time_window.rowsBetween(-11, Window.currentRow)))
    df = df.withColumn("SMA_26", F.avg("close").over(time_window.rowsBetween(-25, Window.currentRow)))
    df = df.withColumn("MACD_Line_SMA_APPROX", F.col("SMA_12") - F.col("SMA_26"))
    df = df.withColumn("MACD_Signal_SMA_APPROX", F.avg("MACD_Line_SMA_APPROX").over(time_window.rowsBetween(-8, Window.currentRow)))
    df = df.withColumn("MACD_Histogram_SMA_APPROX", F.col("MACD_Line_SMA_APPROX") - F.col("MACD_Signal_SMA_APPROX"))

    # 6. On-Balance Volume (OBV)
    obv_step = F.when(F.col("price_diff") > 0, F.col("volume")) \
                .when(F.col("price_diff") < 0, -F.col("volume")) \
                .otherwise(0)
    window_unbounded = Window.partitionBy("ticker").orderBy("timestamp_ms").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("OBV", F.sum(obv_step).over(window_unbounded))

    # Clean intermediate columns
    clean_cols = ["price_diff", "stddev_20", "gain", "loss", "avg_gain", "avg_loss", "SMA_12", "SMA_26"]
    return df.drop(*clean_cols)

def calculate_alpha_101(df: DataFrame) -> DataFrame:
    """
    WorldQuant Alpha#101-style formulas computable from OHLCV alone.

    Alpha#1, #4, #5 from the published "101 Formulaic Alphas" set were
    considered and deliberately NOT added: they need Ts_ArgMax / Ts_Rank
    (the position/percentile-rank of a value within its own trailing
    window) or a VWAP series. Ts_ArgMax/Ts_Rank aren't expressible as a
    single built-in Spark window aggregate -- a correct version needs a
    UDF with no way to verify against a live Spark session here, and a
    wrong-but-confident implementation is worse than an honest gap.
    VWAP only exists for 5m bars in this pipeline (see pandas_indicators.py
    -- VWAP is null for 1d, where the daily bar already IS the session),
    so a VWAP-dependent alpha would silently behave differently per
    interval. Flag if a specific downstream use needs these three.
    """
    time_window = Window.partitionBy("ticker").orderBy("timestamp_ms")
    epsilon = 1e-9

    def cross_section_rank(col):
        """Percentile rank of `col` against every other ticker's bar at the
        same timestamp_ms (the "rank(...)" operator in the WorldQuant spec,
        which is always cross-sectional, not a rolling time-series rank)."""
        return F.percent_rank().over(Window.partitionBy("timestamp_ms").orderBy(col))

    df = df.withColumn("delta_vol_1", F.col("volume") - F.lag("volume", 1).over(time_window))
    df = df.withColumn("delta_close_1", F.col("close") - F.lag("close", 1).over(time_window))

    # Core Alphas
    df = df.withColumn("alpha_12", F.signum("delta_vol_1") * (-1 * F.col("delta_close_1")))
    df = df.withColumn("alpha_54", (-1 * (F.col("low") - F.col("close")) * F.pow(F.col("open"), 5)) / (((F.col("low") - F.col("high")) * F.pow(F.col("close"), 5)) + epsilon))
    df = df.withColumn("typical_price", (F.col("high") + F.col("low") + F.col("close")) / 3)
    df = df.withColumn("alpha_41", F.pow((F.col("high") * F.col("low")), 0.5) - F.col("typical_price"))

    # Alpha#6 (-1 * correlation(open, volume, 10))
    window_10 = time_window.rowsBetween(-9, Window.currentRow)
    df = df.withColumn("alpha_6", -1 * F.corr("open", "volume").over(window_10))

    # Alpha#9 -- WorldQuant formula:
    # (0 < ts_min(delta(close,1), 5)) ? delta(close,1)
    #   : ((ts_max(delta(close,1), 5) < 0) ? delta(close,1) : (-1 * delta(close,1)))
    window_5_delta = time_window.rowsBetween(-4, Window.currentRow)
    df = df.withColumn("ts_min_delta_close_5", F.min("delta_close_1").over(window_5_delta))
    df = df.withColumn("ts_max_delta_close_5", F.max("delta_close_1").over(window_5_delta))
    df = df.withColumn(
        "alpha_9",
        F.when(F.col("ts_min_delta_close_5") > 0, F.col("delta_close_1"))
         .when(F.col("ts_max_delta_close_5") < 0, F.col("delta_close_1"))
         .otherwise(-1 * F.col("delta_close_1"))
    )

    # Alpha#101 ((close - open) / ((high - low) + 0.001))
    df = df.withColumn("alpha_101", (F.col("close") - F.col("open")) / ((F.col("high") - F.col("low")) + 0.001))

    # Alpha#28 Approximation (scale(corr(adv20, low, 5) + (high+low)/2 - close))
    df = df.withColumn("adv20", F.avg("volume").over(time_window.rowsBetween(-19, Window.currentRow)))
    window_5 = time_window.rowsBetween(-4, Window.currentRow)
    df = df.withColumn("alpha_28", (F.corr("adv20", "low").over(window_5) + ((F.col("high") + F.col("low")) / 2)) - F.col("close"))

    # Alpha#2 (-1 * correlation(rank(delta(log(volume), 2)), rank((close - open) / open), 6))
    df = df.withColumn("log_volume", F.log(F.col("volume") + 1))
    df = df.withColumn("delta_log_vol_2", F.col("log_volume") - F.lag("log_volume", 2).over(time_window))
    df = df.withColumn("oc_return", (F.col("close") - F.col("open")) / (F.col("open") + epsilon))
    df = df.withColumn("rank_delta_log_vol_2", cross_section_rank("delta_log_vol_2"))
    df = df.withColumn("rank_oc_return", cross_section_rank("oc_return"))
    window_6 = time_window.rowsBetween(-5, Window.currentRow)
    df = df.withColumn(
        "alpha_2", -1 * F.corr("rank_delta_log_vol_2", "rank_oc_return").over(window_6)
    )

    # Alpha#3 (-1 * correlation(rank(open), rank(volume), 10))
    df = df.withColumn("rank_open", cross_section_rank("open"))
    df = df.withColumn("rank_volume", cross_section_rank("volume"))
    df = df.withColumn("alpha_3", -1 * F.corr("rank_open", "rank_volume").over(window_10))

    drop_cols = [
        "delta_vol_1", "delta_close_1", "typical_price", "adv20",
        "log_volume", "delta_log_vol_2", "oc_return",
        "rank_delta_log_vol_2", "rank_oc_return", "rank_open", "rank_volume",
        "ts_min_delta_close_5", "ts_max_delta_close_5",
    ]
    return df.drop(*drop_cols)

def process_price_stream(df: DataFrame) -> DataFrame:
    df_with_technicals = calculate_technical_indicators(df)
    df_with_alphas = calculate_alpha_101(df_with_technicals)
    return df_with_alphas
