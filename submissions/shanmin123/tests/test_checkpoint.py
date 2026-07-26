import json

from pipeline.checkpoint import CheckpointStore


def test_checkpoint_updates_each_dataset_without_losing_existing_state(tmp_path):
    path = tmp_path / "state" / "checkpoint.json"
    store = CheckpointStore(path)

    assert store.load() == {"prices": {}, "news": {}}

    store.update_price("AAPL", "2025-01-02")
    store.update_news("AAPL", "2025-01-02T12:00:00+00:00")

    assert store.load() == {
        "prices": {"AAPL": {"last_event_date": "2025-01-02"}},
        "news": {"AAPL": {"last_published_at_utc": "2025-01-02T12:00:00+00:00"}},
    }
    assert not path.with_suffix(".json.tmp").exists()
    assert json.loads(path.read_text(encoding="utf-8"))["prices"]["AAPL"]


def test_checkpoint_recovers_from_an_empty_file(tmp_path):
    path = tmp_path / "checkpoint.json"
    path.write_text("", encoding="utf-8")

    assert CheckpointStore(path).load() == {"prices": {}, "news": {}}


def test_concurrent_price_and_news_updates_do_not_lose_each_other(tmp_path):
    """collect-prices and collect-news can run as separate processes against
    one checkpoint file. Unlocked read-modify-write lost one side's update and
    the shared .tmp name raced os.replace into FileNotFoundError."""
    import threading

    from pipeline.checkpoint import CheckpointStore

    store = CheckpointStore(tmp_path / "checkpoint.json")
    errors = []

    def hammer(update, value_format):
        try:
            for index in range(40):
                update("AAPL", value_format % (index % 28 + 1))
        except Exception as exc:  # surfaced through the assertion below
            errors.append(exc)

    threads = [
        threading.Thread(
            target=hammer, args=(store.update_price, "2025-01-%02d")
        ),
        threading.Thread(
            target=hammer,
            args=(store.update_news, "2025-01-02T12:00:%02d+00:00"),
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    state = store.load()
    assert "AAPL" in state["prices"], "price checkpoint lost to the news writer"
    assert "AAPL" in state["news"], "news checkpoint lost to the price writer"


def test_checkpoint_rejects_non_mapping_sections(tmp_path):
    import pytest

    from pipeline.checkpoint import CheckpointStore

    path = tmp_path / "checkpoint.json"
    path.write_text('{"prices": [], "news": {}}', encoding="utf-8")
    with pytest.raises(ValueError, match="prices"):
        CheckpointStore(path).load()
