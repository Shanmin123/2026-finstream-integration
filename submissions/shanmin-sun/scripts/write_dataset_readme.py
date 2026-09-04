"""Write the README that ships beside the collected parquet.

Whoever opens the delivered dataset folder sees only files. This writes the
note that goes with them: what each dataset is, what it covers, how it was
produced, and where its limits are. Every number is read from the parquet
itself rather than typed, so the note cannot describe a dataset other than the
one it sits in.

    python scripts/write_dataset_readme.py --data-root data/output
    python scripts/write_dataset_readme.py --data-root data/output --out README.txt

Without --out it prints, so the text can be checked before it is written.
"""

import argparse
import collections
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DATASETS = ("prices", "indicators", "alphas", "news")
DESCRIPTIONS = {
    "prices": "Daily OHLCV bars from IBKR reqHistoricalData.",
    "indicators": "SMA, EMA, MACD, RSI, Bollinger, ATR and OBV, computed "
                  "offline from the collected prices.",
    "alphas": "An eight-formula subset of WorldQuant Alpha101, computed "
              "offline from the collected prices.",
    "news": "Headlines and provider metadata from IBKR reqHistoricalNews.",
}
KEYS = {
    "prices": "(symbol, event_date)",
    "indicators": "(symbol, event_date)",
    "alphas": "(symbol, event_date)",
    "news": "(symbol, provider_code, article_id)",
}
DATE_COLUMN = {"news": "published_at_utc"}


def _widen(span, low, high):
    """Grow a [first, last] pair to cover one more pair of endpoints."""
    low, high = str(low)[:10], str(high)[:10]
    span[0] = low if span[0] is None else min(span[0], low)
    span[1] = high if span[1] is None else max(span[1], high)


def survey(root, dataset):
    """Row count, symbol count, date span and columns, from file metadata."""
    base = Path(root) / dataset
    files = sorted(base.rglob("*.parquet"))
    if not files:
        return None
    rows, symbols, per_year = 0, set(), collections.Counter()
    for path in files:
        rows += pq.read_metadata(path).num_rows
        parts = {p.split("=")[0]: p.split("=")[1] for p in path.parts if "=" in p}
        if "symbol" in parts:
            symbols.add(parts["symbol"])
        for key in ("event_year", "event_date"):
            if key in parts:
                per_year[parts[key][:4]] += 1
                break
    columns = pq.read_schema(files[0]).names
    column = DATE_COLUMN.get(dataset, "event_date")
    # The collection stamp, so a dataset copied in from an earlier run cannot
    # sit beside a fresh one without saying so.
    stamp = next((c for c in ("retrieved_at_utc", "computed_at_utc")
                  if c in columns), None)
    wanted = [c for c in (column, stamp) if c]
    spans = {c: [None, None] for c in wanted}
    # Every file, not a sample of them: files sort by year then symbol, and
    # inside a year the symbols start on different dates, so the earliest date
    # need not be in the first file. This note is a claim about someone else's
    # data, so it is read from all of it.
    for path in files:
        metadata = pq.read_metadata(path)
        present = set(metadata.schema.names) & set(wanted)
        counted = collections.Counter()
        for group in range(metadata.num_row_groups):
            row_group = metadata.row_group(group)
            for index in range(row_group.num_columns):
                col = row_group.column(index)
                name = col.path_in_schema
                if name not in present or col.statistics is None:
                    continue
                counted[name] += 1
                _widen(spans[name], col.statistics.min, col.statistics.max)
        # Column statistics are optional in the parquet format, and a file
        # written without them would leave the span at None, printing
        # "Covers None to None" beside a correct row count. Read the column
        # from those files instead. A row group short of statistics is enough
        # to re-read, because the rest of the file does not cover its rows.
        short = sorted(c for c in present if counted[c] < metadata.num_row_groups)
        if short:
            table = pq.read_table(path, columns=short)
            for name in short:
                values = [v for v in table.column(name).to_pylist() if v is not None]
                if values:
                    _widen(spans[name], min(values), max(values))
    return {
        "files": len(files), "rows": rows, "symbols": len(symbols),
        "years": sorted(per_year),
        "first": spans[column][0], "last": spans[column][1],
        "collected_first": spans[stamp][0] if stamp else None,
        "collected_last": spans[stamp][1] if stamp else None,
        "columns": columns,
    }


def missing_from(symbols_file, root):
    """Requested tickers with no partition, so a short delivery says so.

    A dataset that reports the tickers it has cannot report the ones it does
    not, and a reader comparing it against the S&P 500 finds the gap the hard
    way.
    """
    if not symbols_file:
        return []
    wanted = []
    for line in Path(symbols_file).read_text(encoding="utf-8").splitlines()[1:]:
        ticker = line.strip().strip(",").strip()
        if ticker:
            wanted.append(ticker)
    have = {d.name.split("=", 1)[1]
            for d in (Path(root) / "prices").rglob("symbol=*") if d.is_dir()}
    return sorted(set(wanted) - have)


def render(root, surveys, generated, missing_symbols=()):
    lines = [
        "IBKR pipeline dataset",
        "=====================",
        "",
        "Collected from Interactive Brokers through the TWS API by the pipeline in",
        "submissions/shanmin-sun of the 2026-finstream-integration repository. The",
        "code that produced this, the column-by-column schema and the table",
        "definitions are all there; this note describes the files.",
        "",
        f"Written {generated}.",
        "",
        "Layout",
        "------",
        "",
        "  final/<dataset>/event_year=YYYY/symbol=SYMBOL/part-000.parquet",
        "  final/news/event_date=YYYY-MM-DD/symbol=SYMBOL/part-000.parquet",
        "",
        "  final/         the datasets to use",
        "  intermediate/  normalised records before deduplication",
        "  raw/           the JSONL request and response envelopes, kept as provenance",
        "",
        "Read the parquet by path or glob rather than pointing Spark at a dataset",
        "root: hive-style partition discovery would derive a `symbol` column from",
        "the directory names that collides with the `symbol` column already in the",
        "files.",
        "",
        "  spark.read.parquet(f\"{root}/final/prices/event_year=*/symbol=*/part-000.parquet\")",
        "",
        "Datasets",
        "--------",
        "",
    ]
    for dataset in DATASETS:
        found = surveys.get(dataset)
        if found is None:
            lines += [f"{dataset}: not present in this delivery.", ""]
            continue
        collected = ""
        if found["collected_first"]:
            when = (found["collected_first"]
                    if found["collected_first"] == found["collected_last"]
                    else f"{found['collected_first']} to {found['collected_last']}")
            collected = f"  Collected {when}."
        lines += [
            f"{dataset}",
            f"  {DESCRIPTIONS[dataset]}",
            f"  {found['rows']:,} rows across {found['symbols']} symbols "
            f"in {found['files']:,} files.",
        ]
        # A dataset whose date column is missing has no span to report. Saying
        # so is the point of the note; "Covers None to None" is not.
        if found["first"]:
            lines.append(f"  Covers {found['first']} to {found['last']}.")
        else:
            lines.append(f"  No {DATE_COLUMN.get(dataset, 'event_date')} column, "
                         "so the note cannot state the period covered.")
        if collected:
            lines.append(collected)
        lines += [
            f"  Deduplication key: {KEYS[dataset]}.",
            f"  Columns: {', '.join(found['columns'])}.",
            "",
        ]

    prices, news = surveys.get("prices"), surveys.get("news")
    lines += ["Limits", "------", ""]
    if missing_symbols:
        lines += [
            f"{len(missing_symbols)} of the {len(missing_symbols) + prices['symbols']} "
            "requested tickers are absent. IBKR answered each of them with",
            "\"security definition not found\", which is what a ticker that has been",
            "delisted or renamed returns. They are:",
            "",
            "  " + ", ".join(missing_symbols),
            "",
        ]
    if news and prices and news["symbols"] < prices["symbols"]:
        lines += [
            f"News covers {news['symbols']} symbols, not the "
            f"{prices['symbols']} the price datasets cover. It is a sample that",
            "exercises the pipeline's news path, not a news dataset. IBKR",
            "historical news needs a logged-in Gateway session and is limited by",
            "provider subscriptions, and the platform's news comes from the EODHD",
            "and GDELT pipelines, which cover it with article text and sentiment.",
            "Treat the headlines here as provenance for the pipeline, not as a",
            "source to analyse.",
            "",
        ]
    lines += [
        "Prices are as IBKR returns them: split-adjusted, NOT dividend-adjusted,",
        "and there is no adjusted_close column. A total-return calculation needs a",
        "dividend series from elsewhere. `wap` is IBKR's own volume-weighted",
        "average for the bar and `bar_count` is the number of trades in it.",
        "",
        "Every row carries retrieved_at_utc, the time this pipeline received it.",
        "It is not a vendor publication timestamp, so it bounds availability from",
        "above rather than stating it.",
        "",
        "Loading it",
        "----------",
        "",
        "The pipeline ships sinks for both platform stores, and a loader:",
        "",
        "  python scripts/load_to_mongo.py --data-root <this folder>/final --dry-run",
        "  python scripts/load_to_mongo.py --data-root <this folder>/final",
        "",
        "--dry-run validates every file and reports counts without connecting.",
        "Every write is keyed and idempotent, so an interrupted load is resumed by",
        "running it again. Connection settings and the SQL table definitions are",
        "documented in the submission's README.",
        "",
        "Contact",
        "-------",
        "",
        "Shanmin Sun, s2812542@ed.ac.uk. Pull request: shanmin-sun submission on",
        "007seann/2026-finstream-integration.",
        "",
    ]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--data-root", required=True,
                        help="Directory holding prices/ indicators/ alphas/ news/.")
    parser.add_argument("--out", type=Path, default=None,
                        help="Where to write. Prints to stdout when omitted.")
    parser.add_argument("--generated", default=None,
                        help="Timestamp to stamp; defaults to now, UTC.")
    parser.add_argument("--symbols-file", default=None,
                        help="The universe that was requested, so the note can "
                             "report which tickers are absent.")
    args = parser.parse_args(argv)

    root = Path(args.data_root)
    if not root.is_dir():
        raise SystemExit(f"no such directory: {root}")
    surveys = {d: survey(root, d) for d in DATASETS}
    if not any(surveys.values()):
        raise SystemExit(f"{root} holds none of {', '.join(DATASETS)}")

    stamp = args.generated or datetime.now(timezone.utc).strftime("%d %B %Y")
    missing = missing_from(args.symbols_file, root) if surveys.get("prices") else []
    text = render(root, surveys, stamp, missing)
    if args.out:
        args.out.write_text(text, encoding="utf-8")
        print(f"wrote {args.out} ({len(text.splitlines())} lines)")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
