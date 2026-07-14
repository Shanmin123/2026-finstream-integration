import json

import pandas as pd

from pipeline.storage import LayeredStorage


def test_price_write_preserves_raw_and_upserts_final_rows(tmp_path):
    storage = LayeredStorage(
        raw_dir=tmp_path / "raw",
        intermediate_dir=tmp_path / "processed",
        final_dir=tmp_path / "output",
    )
    request = {"symbol": "AAPL", "duration": "7 D"}
    rows = [
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
        },
        {
            "event_date": "2025-01-02",
            "symbol": "AAPL",
            "con_id": 265598,
            "exchange": "SMART",
            "currency": "USD",
            "open": 100.0,
            "high": 103.0,
            "low": 99.0,
            "close": 102.0,
            "volume": 1100.0,
            "bar_count": 11,
            "wap": 101.0,
        },
    ]

    first = storage.write_prices(rows, request, "2025-01-03T00:00:00+00:00", "run-1")
    second = storage.write_prices(
        rows[-1:], request, "2025-01-03T01:00:00+00:00", "run-2"
    )

    assert first.input_rows == 2
    assert second.final_rows == 1
    raw_payload = json.loads(first.raw_path.read_text(encoding="utf-8").splitlines()[0])
    assert raw_payload["request"] == request
    final = pd.read_parquet(second.final_paths[0])
    assert list(final["close"]) == [102.0]
    assert list(final["retrieved_at_utc"]) == ["2025-01-03T01:00:00+00:00"]


def test_news_write_deduplicates_by_symbol_provider_and_article(tmp_path):
    storage = LayeredStorage(
        raw_dir=tmp_path / "raw",
        intermediate_dir=tmp_path / "processed",
        final_dir=tmp_path / "output",
    )
    rows = [
        {
            "published_at_utc": "2025-01-02T12:00:00+00:00",
            "symbol": "AAPL",
            "con_id": 265598,
            "provider_code": "BRFG",
            "article_id": "article-1",
            "headline": "Synthetic headline v1",
            "extra_data": "",
        },
        {
            "published_at_utc": "2025-01-02T12:00:00+00:00",
            "symbol": "AAPL",
            "con_id": 265598,
            "provider_code": "BRFG",
            "article_id": "article-1",
            "headline": "Synthetic headline v2",
            "extra_data": "",
        },
    ]

    result = storage.write_news(
        rows,
        {"symbol": "AAPL", "providers": ["BRFG"]},
        "2025-01-02T13:00:00+00:00",
        "run-news",
    )

    final = pd.read_parquet(result.final_paths[0])
    assert result.final_rows == 1
    assert list(final["headline"]) == ["Synthetic headline v2"]
    assert "article_text" not in final.columns
