import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pandas as pd


PRICE_COLUMNS = (
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
NEWS_COLUMNS = (
    "published_at_utc",
    "retrieved_at_utc",
    "symbol",
    "con_id",
    "provider_code",
    "article_id",
    "headline",
    "extra_data",
)


@dataclass(frozen=True)
class WriteResult:
    input_rows: int
    final_rows: int
    raw_path: Path
    intermediate_paths: List[Path]
    final_paths: List[Path]


def _safe_component(value):
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return clean or "unknown"


def _atomic_parquet(frame, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp.parquet")
    frame.to_parquet(temporary, index=False)
    os.replace(str(temporary), str(path))


def _merge_partition(path, incoming, keys, order_column):
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    else:
        combined = incoming.copy()
    combined = combined.drop_duplicates(subset=list(keys), keep="last")
    combined = combined.sort_values(list(keys) + [order_column]).reset_index(drop=True)
    _atomic_parquet(combined, path)
    return combined


def _iso_utc(value):
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat()


class LayeredStorage:
    def __init__(self, raw_dir, intermediate_dir, final_dir):
        self.raw_dir = Path(raw_dir)
        self.intermediate_dir = Path(intermediate_dir)
        self.final_dir = Path(final_dir)

    def _write_raw(self, dataset, rows, request, retrieved_at_utc, run_id):
        retrieval_date = _iso_utc(retrieved_at_utc)[:10]
        path = (
            self.raw_dir
            / dataset
            / ("retrieval_date=" + retrieval_date)
            / (_safe_component(run_id) + ".jsonl")
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "request": request,
            "retrieved_at_utc": _iso_utc(retrieved_at_utc),
            "records": rows,
        }
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(str(temporary), str(path))
        return path

    def _normalize_prices(self, rows, retrieved_at_utc):
        if not rows:
            return pd.DataFrame(columns=PRICE_COLUMNS)
        frame = pd.DataFrame(rows).copy()
        frame["event_date"] = pd.to_datetime(
            frame["event_date"], errors="raise"
        ).dt.strftime("%Y-%m-%d")
        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["retrieved_at_utc"] = _iso_utc(retrieved_at_utc)
        for column in PRICE_COLUMNS:
            if column not in frame.columns:
                frame[column] = None
        frame = frame.loc[:, PRICE_COLUMNS]
        return frame.drop_duplicates(
            subset=["symbol", "event_date"], keep="last"
        ).reset_index(drop=True)

    def _normalize_news(self, rows, retrieved_at_utc):
        if not rows:
            return pd.DataFrame(columns=NEWS_COLUMNS)
        frame = pd.DataFrame(rows).copy()
        frame["published_at_utc"] = frame["published_at_utc"].map(_iso_utc)
        frame["retrieved_at_utc"] = _iso_utc(retrieved_at_utc)
        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["provider_code"] = (
            frame["provider_code"].astype(str).str.strip().str.upper()
        )
        for column in NEWS_COLUMNS:
            if column not in frame.columns:
                frame[column] = None
        frame = frame.loc[:, NEWS_COLUMNS]
        return frame.drop_duplicates(
            subset=["symbol", "provider_code", "article_id"], keep="last"
        ).reset_index(drop=True)

    def write_prices(self, rows, request, retrieved_at_utc, run_id):
        raw_path = self._write_raw("prices", rows, request, retrieved_at_utc, run_id)
        frame = self._normalize_prices(rows, retrieved_at_utc)
        intermediate_paths = []
        final_paths = []
        final_rows = 0
        if not frame.empty:
            for symbol, symbol_frame in frame.groupby("symbol", sort=True):
                intermediate_path = (
                    self.intermediate_dir
                    / "prices"
                    / ("symbol=" + _safe_component(symbol))
                    / "part-000.parquet"
                )
                _merge_partition(
                    intermediate_path,
                    symbol_frame,
                    ("symbol", "event_date"),
                    "retrieved_at_utc",
                )
                intermediate_paths.append(intermediate_path)

                for year, year_frame in symbol_frame.groupby(
                    symbol_frame["event_date"].str[:4], sort=True
                ):
                    final_path = (
                        self.final_dir
                        / "prices"
                        / ("event_year=" + str(year))
                        / ("symbol=" + _safe_component(symbol))
                        / "part-000.parquet"
                    )
                    merged = _merge_partition(
                        final_path,
                        year_frame,
                        ("symbol", "event_date"),
                        "retrieved_at_utc",
                    )
                    final_paths.append(final_path)
                    final_rows += len(merged)
        return WriteResult(
            input_rows=len(rows),
            final_rows=final_rows,
            raw_path=raw_path,
            intermediate_paths=intermediate_paths,
            final_paths=final_paths,
        )

    def read_final_prices(self):
        """Load every final prices partition into one frame (empty when none)."""
        root = self.final_dir / "prices"
        paths = sorted(root.glob("event_year=*/symbol=*/part-000.parquet"))
        frames = [pd.read_parquet(path) for path in paths]
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        return merged.sort_values(["symbol", "event_date"]).reset_index(drop=True)

    def read_latest_quote_last(self):
        """Latest streamed `last` tick per symbol -> {symbol: (value, event_date)}."""
        root = self.final_dir / "quotes"
        paths = sorted(root.glob("event_date=*/symbol=*/part-000.parquet"))
        if not paths:
            return {}
        frame = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
        frame = frame[frame["field"] == "last"]
        if frame.empty:
            return {}
        frame = frame.sort_values("tick_time_utc").groupby("symbol").tail(1)
        return {
            row["symbol"]: (float(row["value"]), str(row["tick_time_utc"])[:10])
            for _, row in frame.iterrows()
        }

    def write_derived(self, dataset, frame, key_columns, order_column):
        """Write a derived (recomputable) dataset to the final layer only.

        Same event_year/symbol partitioning and dedup-merge as prices; no raw
        layer because the inputs are already persisted pipeline outputs.
        """
        final_paths = []
        final_rows = 0
        if frame is not None and not frame.empty:
            for symbol, symbol_frame in frame.groupby("symbol", sort=True):
                for year, year_frame in symbol_frame.groupby(
                    symbol_frame["event_date"].str[:4], sort=True
                ):
                    final_path = (
                        self.final_dir
                        / dataset
                        / ("event_year=" + str(year))
                        / ("symbol=" + _safe_component(symbol))
                        / "part-000.parquet"
                    )
                    merged = _merge_partition(
                        final_path,
                        year_frame,
                        key_columns,
                        order_column,
                    )
                    final_paths.append(final_path)
                    final_rows += len(merged)
        return WriteResult(
            input_rows=0 if frame is None else len(frame),
            final_rows=final_rows,
            raw_path=None,
            intermediate_paths=[],
            final_paths=final_paths,
        )

    QUOTE_COLUMNS = ("tick_time_utc", "symbol", "field", "value", "retrieved_at_utc")

    def write_quotes(self, rows, request, retrieved_at_utc, run_id):
        raw_path = self._write_raw("quotes", rows, request, retrieved_at_utc, run_id)
        frame = pd.DataFrame(rows, columns=list(self.QUOTE_COLUMNS[:-1])) if rows else pd.DataFrame(columns=list(self.QUOTE_COLUMNS))
        final_paths = []
        final_rows = 0
        if not frame.empty:
            frame = frame.copy()
            frame["retrieved_at_utc"] = _iso_utc(retrieved_at_utc)
            frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
            frame = frame.loc[:, list(self.QUOTE_COLUMNS)].drop_duplicates(
                subset=["symbol", "tick_time_utc", "field"], keep="last"
            )
            dates = frame["tick_time_utc"].str[:10]
            for (event_date, symbol), part in frame.groupby([dates, frame["symbol"]], sort=True):
                final_path = (
                    self.final_dir
                    / "quotes"
                    / ("event_date=" + str(event_date))
                    / ("symbol=" + _safe_component(symbol))
                    / "part-000.parquet"
                )
                merged = _merge_partition(
                    final_path,
                    part,
                    ("symbol", "tick_time_utc", "field"),
                    "retrieved_at_utc",
                )
                final_paths.append(final_path)
                final_rows += len(merged)
        return WriteResult(
            input_rows=len(rows),
            final_rows=final_rows,
            raw_path=raw_path,
            intermediate_paths=[],
            final_paths=final_paths,
        )

    def write_news(self, rows, request, retrieved_at_utc, run_id):
        raw_path = self._write_raw("news", rows, request, retrieved_at_utc, run_id)
        frame = self._normalize_news(rows, retrieved_at_utc)
        intermediate_paths = []
        final_paths = []
        final_rows = 0
        if not frame.empty:
            for symbol, symbol_frame in frame.groupby("symbol", sort=True):
                intermediate_path = (
                    self.intermediate_dir
                    / "news"
                    / ("symbol=" + _safe_component(symbol))
                    / "part-000.parquet"
                )
                _merge_partition(
                    intermediate_path,
                    symbol_frame,
                    ("symbol", "provider_code", "article_id"),
                    "retrieved_at_utc",
                )
                intermediate_paths.append(intermediate_path)

                dates = symbol_frame["published_at_utc"].str[:10]
                for event_date, date_frame in symbol_frame.groupby(dates, sort=True):
                    final_path = (
                        self.final_dir
                        / "news"
                        / ("event_date=" + str(event_date))
                        / ("symbol=" + _safe_component(symbol))
                        / "part-000.parquet"
                    )
                    merged = _merge_partition(
                        final_path,
                        date_frame,
                        ("symbol", "provider_code", "article_id"),
                        "retrieved_at_utc",
                    )
                    final_paths.append(final_path)
                    final_rows += len(merged)
        return WriteResult(
            input_rows=len(rows),
            final_rows=final_rows,
            raw_path=raw_path,
            intermediate_paths=intermediate_paths,
            final_paths=final_paths,
        )
