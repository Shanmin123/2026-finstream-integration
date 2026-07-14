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
