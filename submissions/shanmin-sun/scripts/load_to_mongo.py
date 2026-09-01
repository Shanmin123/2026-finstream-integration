"""Load the pipeline's parquet datasets into the platform's MongoDB.

The collector writes parquet; this is the one command that moves those files
into the shared database. It is separate from the collector on purpose: the
parquet output is the primary artefact and stays valid whether or not a
database is reachable, and a load can be re-run against the same files without
re-contacting IBKR.

    python scripts/load_to_mongo.py --data-root /path/to/data/output --dry-run
    python scripts/load_to_mongo.py --data-root /path/to/data/output

Every write is idempotent: prices leave an existing bar alone (the platform's
primary feed wins) and derived values are replaced only by a newer computation,
so an interrupted load is resumed by running it again.

News is not loaded. The platform defines no news collection, its news comes
from the EODHD and GDELT pipelines, and this pipeline's news is a two-ticker
sample; it is delivered as parquet only. See the dataset README.

Connection settings come from the environment, documented in
pipeline/mongo_sink.py. --dry-run reads and validates every file and reports
what would be written without opening a connection.
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.mongo_sink import (  # noqa: E402
    derived_documents,
    ensure_indexes,
    get_database,
    price_documents,
    record_run,
    write_alphas,
    write_indicators,
    write_prices,
)

logger = logging.getLogger("ibkr_pipeline")

# dataset directory -> (how to turn a frame into documents, how to write it)
DATASETS = {
    "prices": {
        "documents": lambda frame: price_documents(frame.to_dict("records")),
        "write": lambda frame, database: write_prices(
            frame.to_dict("records"), database
        ),
    },
    "indicators": {
        "documents": lambda frame: derived_documents(frame, "indicator_name"),
        "write": lambda frame, database: write_indicators(
            frame, database_factory=database
        ),
    },
    "alphas": {
        "documents": lambda frame: derived_documents(frame, "alpha_id"),
        "write": lambda frame, database: write_alphas(frame, database_factory=database),
    },
}


def parquet_files(root, dataset):
    base = Path(root) / dataset
    if not base.is_dir():
        return []
    return sorted(base.rglob("*.parquet"))


def batched(files, batch_size):
    """Group symbol files into batches, so one bulk_write covers several files.

    Batching by file rather than by row keeps a symbol's rows together, which
    matters when a load is interrupted: a partly written symbol is repaired by
    the next run because every write is keyed and idempotent.
    """
    batch, rows = [], 0
    for path in files:
        frame = pd.read_parquet(path)
        if frame.empty:
            continue
        batch.append(frame)
        rows += len(frame)
        if rows >= batch_size:
            yield pd.concat(batch, ignore_index=True)
            batch, rows = [], 0
    if batch:
        yield pd.concat(batch, ignore_index=True)


def load_dataset(dataset, root, batch_size, database_factory, dry_run):
    spec = DATASETS[dataset]
    files = parquet_files(root, dataset)
    if not files:
        return {"dataset": dataset, "files": 0, "documents": 0, "written": 0,
                "skipped": 0}

    documents = written = skipped = 0
    for frame in batched(files, batch_size):
        prepared = spec["documents"](frame)
        # derived_documents drops NaN warm-up cells; the difference between the
        # rows read and the documents built is reported rather than inferred.
        expected = len(frame) if dataset == "prices" else None
        documents += len(prepared)
        if expected is not None:
            skipped += expected - len(prepared)
        if not dry_run:
            written += spec["write"](frame, database_factory)
    return {
        "dataset": dataset,
        "files": len(files),
        "documents": documents,
        "written": written,
        "skipped": skipped,
    }


SMOKE_TICKER = "__IBKR_SMOKE__"


def smoke_test(database_factory=get_database):
    """Prove the connection and the conflict policy on a handful of documents.

    The full load is millions of documents. This is what to run first against a
    database nobody here has reached before: it writes two rows under a ticker
    no exchange lists, checks that a second identical write adds nothing, and
    removes exactly what it wrote. It touches no real ticker.
    """
    from pipeline.mongo_sink import PRICE_COLLECTION, PRICE_KEY, price_documents

    rows = [
        {"event_date": "2000-01-03", "symbol": SMOKE_TICKER, "open": 1.0,
         "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1.0},
        {"event_date": "2000-01-04", "symbol": SMOKE_TICKER, "open": 1.5,
         "high": 2.5, "low": 1.0, "close": 2.0, "volume": 2.0},
    ]
    documents = price_documents(rows)
    keys = [{field: d[field] for field in PRICE_KEY} for d in documents]
    database = database_factory()
    collection = database[PRICE_COLLECTION]
    try:
        first = write_prices(rows, database_factory)
        found = collection.count_documents({"ticker": SMOKE_TICKER})
        second = write_prices(rows, database_factory)
        again = collection.count_documents({"ticker": SMOKE_TICKER})
    finally:
        # Only the two documents this function wrote, by their exact keys.
        removed = sum(collection.delete_one(key).deleted_count for key in keys)
    return {
        "wrote": first,
        "found": found,
        "second_write_added": second,
        "count_after_second_write": again,
        "idempotent": found == again == len(rows) and second == 0,
        "cleaned_up": removed,
    }


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--data-root",
        help="The collector's output directory, holding prices/ indicators/ alphas/.",
    )
    parser.add_argument(
        "--smoke-test", action="store_true",
        help="Write, re-write and remove two documents under a placeholder "
             "ticker, to check the connection and the conflict policy before "
             "loading millions. Needs no --data-root.",
    )
    parser.add_argument(
        "--datasets", default="prices,indicators,alphas",
        help="Comma-separated subset to load.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50_000,
        help="Rows per bulk write. Lower it if the server rejects large batches.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read and validate every file, report counts, open no connection.",
    )
    parser.add_argument(
        "--skip-indexes", action="store_true",
        help="Do not create the unique indexes (they already exist).",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.smoke_test:
        try:
            report = smoke_test(get_database)
        except Exception as exc:
            print(json.dumps({"status": "failed", "error": str(exc),
                              "type": type(exc).__name__}), file=sys.stderr)
            return 1
        print(json.dumps({"status": "smoke_test", **report}, indent=2))
        return 0 if report["idempotent"] else 1

    if not args.data_root:
        print(json.dumps({"status": "failed",
                          "error": "--data-root is required unless --smoke-test"}),
              file=sys.stderr)
        return 1

    root = Path(args.data_root)
    if not root.is_dir():
        print(json.dumps({"status": "failed", "error": f"no such directory: {root}"}),
              file=sys.stderr)
        return 1

    requested = [d.strip() for d in args.datasets.split(",") if d.strip()]
    unknown = [d for d in requested if d not in DATASETS]
    if unknown:
        print(json.dumps({"status": "failed", "error": f"unknown datasets: {unknown}"}),
              file=sys.stderr)
        return 1

    database_factory = get_database
    started = time.monotonic()
    try:
        if not args.dry_run and not args.skip_indexes:
            ensure_indexes(database_factory)
        summaries = [
            load_dataset(d, root, args.batch_size, database_factory, args.dry_run)
            for d in requested
        ]
    except Exception as exc:
        elapsed = time.monotonic() - started
        # A failed load is recorded too, when the database is the thing that
        # still answers; otherwise the run leaves no trace of having tried.
        if not args.dry_run:
            try:
                record_run("ibkr_mongo_load", 0, elapsed, "failed", str(exc),
                           database_factory)
            except Exception:
                logger.warning("run_record_failed", exc_info=True)
        print(json.dumps({"status": "failed", "error": str(exc),
                          "type": type(exc).__name__}), file=sys.stderr)
        return 1

    elapsed = time.monotonic() - started
    total = sum(s["written"] for s in summaries)
    if not args.dry_run:
        record_run("ibkr_mongo_load", total, elapsed, "success",
                   database_factory=database_factory)
    print(json.dumps(
        {
            "status": "dry_run" if args.dry_run else "success",
            "elapsed_seconds": round(elapsed, 1),
            "documents_written": total,
            "datasets": summaries,
        },
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
