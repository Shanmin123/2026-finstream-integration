import json

import pandas as pd
import pytest

from scripts import load_to_mongo


@pytest.fixture
def dataset_root(tmp_path):
    """Two symbols, one year, all three datasets, in the collector's layout."""
    days = ["2025-01-02", "2025-01-03"]
    for symbol in ("AAPL", "MSFT"):
        prices = pd.DataFrame([
            {"event_date": d, "symbol": symbol, "open": 1.0, "high": 2.0,
             "low": 0.5, "close": 1.5, "volume": 10.0} for d in days
        ])
        indicators = pd.DataFrame([
            {"symbol": symbol, "event_date": d, "rsi_14": 55.0, "sma_20": 1.2,
             "computed_at_utc": "2026-07-25T00:00:00+00:00"} for d in days
        ])
        alphas = pd.DataFrame([
            {"symbol": symbol, "event_date": d, "alpha_6": 0.1,
             "computed_at_utc": "2026-07-25T00:00:00+00:00"} for d in days
        ])
        for name, frame in (("prices", prices), ("indicators", indicators),
                            ("alphas", alphas)):
            directory = tmp_path / name / "event_year=2025" / f"symbol={symbol}"
            directory.mkdir(parents=True)
            frame.to_parquet(directory / "part-000.parquet", index=False)
    return tmp_path


def run(argv, capsys):
    code = load_to_mongo.main(argv)
    return code, capsys.readouterr()


def test_dry_run_counts_documents_without_connecting(dataset_root, capsys,
                                                     monkeypatch):
    def explode(*_a, **_k):
        raise AssertionError("--dry-run must not touch the database")

    monkeypatch.setattr(load_to_mongo, "ensure_indexes", explode)
    monkeypatch.setattr(load_to_mongo, "write_prices", explode)
    monkeypatch.setattr(load_to_mongo, "record_run", explode)

    code, out = run(["--data-root", str(dataset_root), "--dry-run"], capsys)
    assert code == 0
    report = json.loads(out.out)
    assert report["status"] == "dry_run" and report["documents_written"] == 0
    counts = {d["dataset"]: d["documents"] for d in report["datasets"]}
    # 4 rows each; indicators carry two values per row, alphas one.
    assert counts == {"prices": 4, "indicators": 8, "alphas": 4}
    # Rows are reported alongside, so the long-layout expansion is visible
    # rather than looking like the loader invented documents.
    assert {d["dataset"]: d["rows"] for d in report["datasets"]} == {
        "prices": 4, "indicators": 4, "alphas": 4
    }


def test_a_real_run_writes_every_dataset_and_records_the_run(dataset_root, capsys,
                                                             monkeypatch):
    calls = []
    monkeypatch.setattr(load_to_mongo, "ensure_indexes", lambda f: calls.append("idx"))
    for name in ("write_prices", "write_indicators", "write_alphas"):
        monkeypatch.setattr(
            load_to_mongo, name,
            lambda *a, _n=name, **k: (calls.append(_n), 7)[1],
        )
    runs = []
    monkeypatch.setattr(
        load_to_mongo, "record_run",
        lambda *a, **k: runs.append(a),
    )

    code, out = run(["--data-root", str(dataset_root)], capsys)
    assert code == 0
    report = json.loads(out.out)
    assert report["status"] == "success" and report["documents_written"] == 21
    assert calls == ["idx", "write_prices", "write_indicators", "write_alphas"]
    assert runs and runs[0][0] == "ibkr_mongo_load" and runs[0][3] == "success"


def test_indexes_can_be_skipped_when_they_already_exist(dataset_root, capsys,
                                                        monkeypatch):
    def explode(*_a, **_k):
        raise AssertionError("--skip-indexes must not create indexes")

    monkeypatch.setattr(load_to_mongo, "ensure_indexes", explode)
    for name in ("write_prices", "write_indicators", "write_alphas"):
        monkeypatch.setattr(load_to_mongo, name, lambda *a, **k: 0)
    monkeypatch.setattr(load_to_mongo, "record_run", lambda *a, **k: None)

    code, _ = run(["--data-root", str(dataset_root), "--skip-indexes"], capsys)
    assert code == 0


def test_a_subset_loads_only_what_was_asked_for(dataset_root, capsys, monkeypatch):
    monkeypatch.setattr(load_to_mongo, "ensure_indexes", lambda f: None)
    monkeypatch.setattr(load_to_mongo, "record_run", lambda *a, **k: None)
    for name in ("write_indicators", "write_alphas"):
        monkeypatch.setattr(
            load_to_mongo, name,
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("not requested")),
        )
    monkeypatch.setattr(load_to_mongo, "write_prices", lambda *a, **k: 4)

    code, out = run(
        ["--data-root", str(dataset_root), "--datasets", "prices"], capsys
    )
    assert code == 0
    assert [d["dataset"] for d in json.loads(out.out)["datasets"]] == ["prices"]


def test_a_missing_dataset_directory_is_reported_not_crashed(tmp_path, capsys,
                                                             monkeypatch):
    (tmp_path / "prices").mkdir()
    monkeypatch.setattr(load_to_mongo, "ensure_indexes", lambda f: None)
    monkeypatch.setattr(load_to_mongo, "record_run", lambda *a, **k: None)
    code, out = run(["--data-root", str(tmp_path), "--dry-run"], capsys)
    assert code == 0
    assert all(d["files"] == 0 for d in json.loads(out.out)["datasets"])


def test_a_bad_root_and_a_bad_dataset_name_fail_with_json_on_stderr(tmp_path,
                                                                    capsys):
    code, out = run(["--data-root", str(tmp_path / "nope")], capsys)
    assert code == 1 and json.loads(out.err)["status"] == "failed"

    code, out = run(
        ["--data-root", str(tmp_path), "--datasets", "prices,quotes"], capsys
    )
    assert code == 1 and "quotes" in json.loads(out.err)["error"]


def test_a_write_failure_is_recorded_before_it_is_reported(dataset_root, capsys,
                                                           monkeypatch):
    """The database is usually still reachable when a load fails, and a run
    that leaves no trace of having tried is worse than one that failed."""
    monkeypatch.setattr(load_to_mongo, "ensure_indexes", lambda f: None)
    monkeypatch.setattr(
        load_to_mongo, "write_prices",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("connection reset")),
    )
    recorded = []
    monkeypatch.setattr(load_to_mongo, "record_run",
                        lambda *a, **k: recorded.append(a))

    code, out = run(["--data-root", str(dataset_root)], capsys)
    assert code == 1
    assert json.loads(out.err)["error"] == "connection reset"
    assert recorded and recorded[0][3] == "failed"


def test_a_failure_while_recording_the_failure_still_reports_the_original(
    dataset_root, capsys, monkeypatch
):
    monkeypatch.setattr(load_to_mongo, "ensure_indexes", lambda f: None)
    monkeypatch.setattr(
        load_to_mongo, "write_prices",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("connection reset")),
    )
    monkeypatch.setattr(
        load_to_mongo, "record_run",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("also down")),
    )
    code, out = run(["--data-root", str(dataset_root)], capsys)
    assert code == 1
    assert json.loads(out.err)["error"] == "connection reset"


def test_empty_parquet_files_are_skipped_not_concatenated(tmp_path):
    directory = tmp_path / "prices" / "event_year=2025" / "symbol=AAPL"
    directory.mkdir(parents=True)
    pd.DataFrame(columns=["event_date", "symbol"]).to_parquet(
        directory / "part-000.parquet", index=False
    )
    assert list(load_to_mongo.batched(
        load_to_mongo.parquet_files(tmp_path, "prices"), 10
    )) == []


class SmokeCollection:
    def __init__(self, store):
        self.store = store
        self.documents = []

    def count_documents(self, query):
        return sum(1 for d in self.documents if d["ticker"] == query["ticker"])

    def delete_one(self, key):
        for index, document in enumerate(self.documents):
            if all(document[k] == v for k, v in key.items()):
                self.documents.pop(index)
                return type("R", (), {"deleted_count": 1})()
        return type("R", (), {"deleted_count": 0})()


class SmokeDatabase:
    def __init__(self, collection):
        self.collection = collection

    def __getitem__(self, _name):
        return self.collection


def test_the_smoke_test_writes_rewrites_and_removes_only_its_own_rows(monkeypatch):
    """The first live contact should be two documents, not millions, and it
    must leave the database exactly as it found it."""
    collection = SmokeCollection([])
    database = SmokeDatabase(collection)

    def fake_write(rows, factory):
        from pipeline.mongo_sink import PRICE_KEY, price_documents

        written = 0
        for document in price_documents(rows):
            key = {f: document[f] for f in PRICE_KEY}
            if not any(all(d[k] == v for k, v in key.items())
                       for d in collection.documents):
                collection.documents.append(document)
                written += 1
        return written

    monkeypatch.setattr(load_to_mongo, "write_prices", fake_write)
    report = load_to_mongo.smoke_test(lambda: database)

    assert report["wrote"] == 2 and report["found"] == 2
    assert report["second_write_added"] == 0, "the conflict policy did not hold"
    assert report["idempotent"] is True
    assert report["cleaned_up"] == 2
    assert collection.documents == [], "the smoke test left rows behind"


def test_the_smoke_test_cleans_up_even_when_the_write_fails(monkeypatch):
    collection = SmokeCollection([])
    monkeypatch.setattr(
        load_to_mongo, "write_prices",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("connection reset")),
    )
    with pytest.raises(RuntimeError):
        load_to_mongo.smoke_test(lambda: SmokeDatabase(collection))
    assert collection.documents == []


def test_a_failing_cleanup_does_not_hide_why_the_write_failed(monkeypatch):
    """The reason the write failed is what the operator needs. A cleanup that
    raises inside `finally` would replace it with a delete error."""

    class Unremovable(SmokeCollection):
        def delete_one(self, key):
            raise RuntimeError("delete not permitted")

    monkeypatch.setattr(
        load_to_mongo, "write_prices",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("connection reset")),
    )
    with pytest.raises(RuntimeError, match="connection reset"):
        load_to_mongo.smoke_test(lambda: SmokeDatabase(Unremovable([])))


def test_data_root_is_only_required_for_a_real_load(capsys):
    code, out = run([], capsys)
    assert code == 1 and "--data-root is required" in json.loads(out.err)["error"]


def test_batching_keeps_a_symbol_whole_and_covers_every_row(dataset_root):
    files = load_to_mongo.parquet_files(dataset_root, "prices")
    batches = list(load_to_mongo.batched(files, batch_size=1))
    # batch_size 1 still emits whole files, never half of one.
    assert len(batches) == len(files)
    assert sum(len(b) for b in batches) == 4
    assert set(pd.concat(batches)["symbol"]) == {"AAPL", "MSFT"}


def test_smoke_test_exits_nonzero_when_the_write_is_not_idempotent(monkeypatch,
                                                                   capsys):
    """--smoke-test is a gate: it is run before a load to prove the server
    honours the conflict policies. A gate that returns 0 whatever it found is
    not a gate."""
    monkeypatch.setattr(load_to_mongo, "get_database", lambda: None)

    monkeypatch.setattr(load_to_mongo, "smoke_test",
                        lambda factory: {"idempotent": True, "found": 2})
    assert load_to_mongo.main(["--smoke-test"]) == 0
    assert '"status": "smoke_test"' in capsys.readouterr().out

    monkeypatch.setattr(load_to_mongo, "smoke_test",
                        lambda factory: {"idempotent": False, "found": 4})
    assert load_to_mongo.main(["--smoke-test"]) == 1


def test_smoke_test_reports_a_failure_rather_than_raising(monkeypatch, capsys):
    def explode(factory):
        raise RuntimeError("no server")

    monkeypatch.setattr(load_to_mongo, "get_database", lambda: None)
    monkeypatch.setattr(load_to_mongo, "smoke_test", explode)
    assert load_to_mongo.main(["--smoke-test"]) == 1
    captured = capsys.readouterr()
    assert "no server" in captured.err
    assert captured.out == "", "a failure must not print a success report"
