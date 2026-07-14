from datetime import datetime, timezone

from pyspark.sql import SparkSession

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
    try:
        prices = spark.read.parquet(str(price_result.final_paths[0]))
        news = spark.read.parquet(str(news_result.final_paths[0]))
        assert prices.count() == 1
        assert news.count() == 1
        assert prices.select("symbol").first()[0] == "AAPL"
        assert news.select("provider_code").first()[0] == "BRFG"
        assert "event_date" in prices.columns
        assert "published_at_utc" in news.columns
    finally:
        spark.stop()
