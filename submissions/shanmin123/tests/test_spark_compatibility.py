from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from pipeline.features import compute_alphas, compute_indicators
from pipeline.storage import LayeredStorage


def test_spark_351_reads_final_partitioned_parquet(pipeline_config):
    storage = LayeredStorage(
        pipeline_config.paths.raw_data_dir,
        pipeline_config.paths.intermediate_data_dir,
        pipeline_config.paths.output_dir,
    )
    retrieved_at = datetime(2025, 1, 3, tzinfo=timezone.utc).isoformat()
    price_result = storage.write_prices(
        [
            {
                "event_date": "2025-01-02",
                "symbol": "AAPL",
                "con_id": 265598,
                "exchange": "SMART",
                "currency": "USD",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000.0,
                "bar_count": 10,
                "wap": 100.5,
            }
        ],
        {"symbol": "AAPL", "duration": "7 D"},
        retrieved_at,
        "spark-price-test",
    )
    news_result = storage.write_news(
        [
            {
                "published_at_utc": "2025-01-02T12:00:00+00:00",
                "symbol": "AAPL",
                "con_id": 265598,
                "provider_code": "BRFG",
                "article_id": "synthetic-1",
                "headline": "Synthetic headline",
            }
        ],
        {"symbol": "AAPL", "providers": ["BRFG"]},
        retrieved_at,
        "spark-news-test",
    )

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("ibkr-parquet-compatibility-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    # Derived datasets share the same partition writer; verify Spark reads them too.
    n = 30
    dates = pd.date_range("2025-01-02", periods=n, freq="B").strftime("%Y-%m-%d")
    close = pd.Series(100.0 + np.arange(n))
    synthetic = pd.DataFrame(
        {
            "symbol": "AAPL",
            "event_date": dates,
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": 1000.0,
        }
    )
    indicators_result = storage.write_derived(
        "indicators",
        compute_indicators(synthetic, retrieved_at),
        ("symbol", "event_date"),
        "computed_at_utc",
    )
    alphas_result = storage.write_derived(
        "alphas",
        compute_alphas(synthetic, retrieved_at),
        ("symbol", "event_date"),
        "computed_at_utc",
    )
    # The streaming outputs use the same writers; cover their schemas too.
    from pipeline.features import compute_live_features

    quotes_result = storage.write_quotes(
        [
            {"tick_time_utc": "2025-02-14T14:30:00+00:00",
             "session_date": "2025-02-14", "symbol": "AAPL",
             "field": "last", "value": 130.0},
        ],
        {"symbols": ["AAPL"]},
        retrieved_at,
        "spark-quotes-test",
    )
    live_i, live_a = compute_live_features(
        synthetic,
        {"AAPL": {"event_date": "2025-02-14", "close": 130.0}},
        retrieved_at,
    )
    live_i_result = storage.write_derived(
        "indicators_live", live_i, ("symbol", "event_date"), "as_of_utc"
    )
    live_a_result = storage.write_derived(
        "alphas_live", live_a, ("symbol", "event_date"), "as_of_utc"
    )

    try:
        prices = spark.read.parquet(str(price_result.final_paths[0]))
        news = spark.read.parquet(str(news_result.final_paths[0]))
        indicators = spark.read.parquet(str(indicators_result.final_paths[0]))
        alphas = spark.read.parquet(str(alphas_result.final_paths[0]))
        assert prices.count() == 1
        assert news.count() == 1
        assert prices.select("symbol").first()[0] == "AAPL"
        assert news.select("provider_code").first()[0] == "BRFG"
        assert "event_date" in prices.columns
        assert "published_at_utc" in news.columns
        assert indicators.count() == 30
        assert "rsi_14" in indicators.columns
        assert alphas.count() == 30
        assert "alpha_101" in alphas.columns

        quotes = spark.read.parquet(str(quotes_result.final_paths[0]))
        assert quotes.count() == 1
        assert {"tick_time_utc", "session_date", "field", "value"} <= set(quotes.columns)
        live_indicators = spark.read.parquet(str(live_i_result.final_paths[0]))
        live_alphas = spark.read.parquet(str(live_a_result.final_paths[0]))
        assert live_indicators.count() == 1
        assert bool(live_indicators.select("provisional").first()[0]) is True
        assert "as_of_utc" in live_indicators.columns
        assert live_alphas.count() == 1
    finally:
        spark.stop()
