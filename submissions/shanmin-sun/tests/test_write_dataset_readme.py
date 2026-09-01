import pandas as pd
import pytest

from scripts import write_dataset_readme as writer


def build(root, dataset, frames):
    """frames: {(year, symbol): DataFrame}"""
    for (year, symbol), frame in frames.items():
        directory = root / dataset / f"event_year={year}" / f"symbol={symbol}"
        directory.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(directory / "part-000.parquet", index=False)


def price_frame(symbol, dates):
    return pd.DataFrame([
        {"event_date": d, "symbol": symbol, "open": 1.0, "high": 2.0, "low": 0.5,
         "close": 1.5, "volume": 10.0, "wap": 1.4, "bar_count": 3,
         "retrieved_at_utc": "2026-09-01T00:00:00+00:00"}
        for d in dates
    ])


@pytest.fixture
def dataset_root(tmp_path):
    build(tmp_path, "prices", {
        ("2025", "AAPL"): price_frame("AAPL", ["2025-01-02", "2025-01-03"]),
        ("2026", "AAPL"): price_frame("AAPL", ["2026-01-02"]),
        ("2025", "MSFT"): price_frame("MSFT", ["2025-01-02", "2025-01-03"]),
    })
    news = pd.DataFrame([{
        "published_at_utc": "2026-05-01T14:00:00+00:00",
        "retrieved_at_utc": "2026-09-01T00:00:00+00:00", "symbol": "AAPL",
        "con_id": 1, "provider_code": "BRFG", "article_id": "a1",
        "headline": "something",
    }])
    directory = tmp_path / "news" / "event_date=2026-05-01" / "symbol=AAPL"
    directory.mkdir(parents=True)
    news.to_parquet(directory / "part-000.parquet", index=False)
    return tmp_path


def test_the_survey_counts_rows_symbols_and_the_span(dataset_root):
    found = writer.survey(dataset_root, "prices")
    assert found["rows"] == 5
    assert found["symbols"] == 2
    assert found["files"] == 3
    assert (found["first"], found["last"]) == ("2025-01-02", "2026-01-02")
    assert "wap" in found["columns"]


def test_news_is_surveyed_on_its_own_date_column(dataset_root):
    """News partitions by event_date and carries published_at_utc, not
    event_date; surveying it with the price column would find nothing."""
    found = writer.survey(dataset_root, "news")
    assert found["rows"] == 1 and found["first"] == "2026-05-01"


def test_an_absent_dataset_surveys_as_none(tmp_path):
    assert writer.survey(tmp_path, "alphas") is None


def test_the_note_reports_the_numbers_it_measured(dataset_root, capsys):
    writer.main(["--data-root", str(dataset_root)])
    text = capsys.readouterr().out
    assert "814,022" not in text, "numbers must come from this folder, not memory"
    assert "5 rows across 2 symbols in 3 files" in text
    assert "2025-01-02 to 2026-01-02" in text
    assert "(symbol, event_date)" in text


def test_a_missing_dataset_is_named_rather_than_omitted(dataset_root, capsys):
    """A dataset silently absent from the note reads as one that was never
    part of the delivery."""
    writer.main(["--data-root", str(dataset_root)])
    text = capsys.readouterr().out
    assert "alphas: not present in this delivery." in text
    assert "indicators: not present in this delivery." in text


def test_thin_news_coverage_is_stated_as_a_limit(dataset_root, capsys):
    writer.main(["--data-root", str(dataset_root)])
    text = capsys.readouterr().out
    assert "News covers 1 symbols, not the 2" in text
    assert "not a news dataset" in text


def test_news_matching_the_price_universe_raises_no_limit(tmp_path, capsys):
    build(tmp_path, "prices", {("2025", "AAPL"): price_frame("AAPL", ["2025-01-02"])})
    directory = tmp_path / "news" / "event_date=2025-01-02" / "symbol=AAPL"
    directory.mkdir(parents=True)
    pd.DataFrame([{
        "published_at_utc": "2025-01-02T10:00:00+00:00",
        "retrieved_at_utc": "2026-09-01T00:00:00+00:00", "symbol": "AAPL",
        "con_id": 1, "provider_code": "BRFG", "article_id": "a1",
        "headline": "x",
    }]).to_parquet(directory / "part-000.parquet", index=False)
    writer.main(["--data-root", str(tmp_path)])
    assert "not a news dataset" not in capsys.readouterr().out


def test_the_adjustment_caveat_is_always_stated(dataset_root, capsys):
    """Prices are split-adjusted but not dividend-adjusted, and a reader who
    does not know that computes total return wrong."""
    writer.main(["--data-root", str(dataset_root)])
    text = capsys.readouterr().out
    assert "NOT dividend-adjusted" in text
    assert "no adjusted_close column" in text


def test_writing_to_a_file_reports_it(dataset_root, tmp_path, capsys):
    out = tmp_path / "README.txt"
    writer.main(["--data-root", str(dataset_root), "--out", str(out)])
    assert out.exists() and "IBKR pipeline dataset" in out.read_text(encoding="utf-8")
    assert "wrote" in capsys.readouterr().out


def test_the_timestamp_can_be_pinned_so_the_note_is_reproducible(dataset_root,
                                                                 capsys):
    writer.main(["--data-root", str(dataset_root), "--generated", "01 January 2026"])
    assert "Written 01 January 2026." in capsys.readouterr().out


def test_a_root_with_none_of_the_datasets_is_an_error(tmp_path):
    (tmp_path / "something_else").mkdir()
    with pytest.raises(SystemExit):
        writer.main(["--data-root", str(tmp_path)])
    with pytest.raises(SystemExit):
        writer.main(["--data-root", str(tmp_path / "nope")])
