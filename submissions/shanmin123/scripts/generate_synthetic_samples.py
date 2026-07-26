import csv
from pathlib import Path


PRICE_FIELDS = (
    "event_date",
    "symbol",
    "con_id",
    "exchange",
    "currency",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "bar_count",
    "wap",
    "retrieved_at_utc",
)
NEWS_FIELDS = (
    "published_at_utc",
    "retrieved_at_utc",
    "symbol",
    "con_id",
    "provider_code",
    "article_id",
    "headline",
)


def write_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    output_dir = Path(__file__).resolve().parents[1] / "samples"
    write_csv(
        output_dir / "synthetic_prices.csv",
        PRICE_FIELDS,
        [
            {
                "event_date": "2025-01-02",
                "symbol": "DEMO",
                "con_id": 100001,
                "exchange": "SMART",
                "currency": "USD",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000.0,
                "bar_count": 10,
                "wap": 100.5,
                "retrieved_at_utc": "2025-01-03T00:00:00+00:00",
            },
            {
                "event_date": "2025-01-03",
                "symbol": "DEMO",
                "con_id": 100001,
                "exchange": "SMART",
                "currency": "USD",
                "open": 101.0,
                "high": 103.0,
                "low": 100.0,
                "close": 102.0,
                "volume": 1200.0,
                "bar_count": 12,
                "wap": 101.6,
                "retrieved_at_utc": "2025-01-04T00:00:00+00:00",
            },
        ],
    )
    write_csv(
        output_dir / "synthetic_news.csv",
        NEWS_FIELDS,
        [
            {
                "published_at_utc": "2025-01-02T12:00:00+00:00",
                "retrieved_at_utc": "2025-01-02T12:05:00+00:00",
                "symbol": "DEMO",
                "con_id": 100001,
                "provider_code": "DEMO",
                "article_id": "synthetic-1",
                "headline": "Synthetic company update",
            },
            {
                "published_at_utc": "2025-01-03T12:00:00+00:00",
                "retrieved_at_utc": "2025-01-03T12:05:00+00:00",
                "symbol": "DEMO",
                "con_id": 100001,
                "provider_code": "DEMO",
                "article_id": "synthetic-2",
                "headline": "Synthetic analyst headline",
            },
        ],
    )


if __name__ == "__main__":
    main()
