from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pandas as pd

DEFAULT_SOURCE = Path("data/raw/lenta-ru-news.csv.bz2")
DEFAULT_OUTPUT = Path("data/demo/lenta_demo.csv")
OUTPUT_COLUMNS = [
    "news_id",
    "url",
    "title",
    "text",
    "topic",
    "tags",
    "published_at",
    "language",
]
READ_CHUNK_SIZE = 100_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the deterministic recent Lenta.ru corpus used by demo mode."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rows", type=int, default=250)
    return parser.parse_args()


def build_fixture(source: Path, output: Path, rows: int) -> pd.DataFrame:
    if rows < 1:
        raise ValueError("rows must be positive")
    if not source.is_file():
        raise FileNotFoundError(source)

    selected = _select_latest_rows(source, rows)
    if len(selected) != rows:
        raise ValueError(f"Could select only {len(selected)} of {rows} requested rows")

    selected["language"] = "ru"
    selected["published_at"] = selected["published_at"].dt.strftime("%Y-%m-%d")
    output.parent.mkdir(parents=True, exist_ok=True)
    selected[OUTPUT_COLUMNS].to_csv(output, index=False)
    return selected[OUTPUT_COLUMNS]


def _select_latest_rows(source: Path, rows: int) -> pd.DataFrame:
    latest = pd.DataFrame()
    for chunk in pd.read_csv(
        source,
        compression="infer",
        chunksize=READ_CHUNK_SIZE,
        index_col=False,
        low_memory=False,
    ):
        normalized = _normalize_source_chunk(chunk)
        latest = pd.concat([latest, normalized], ignore_index=True)
        latest = (
            latest.drop_duplicates("url", keep="last")
            .sort_values(["published_at", "url"], kind="stable")
            .tail(rows)
        )

    return latest.sort_values(["published_at", "url"], kind="stable").reset_index(
        drop=True
    )


def _normalize_source_chunk(frame: pd.DataFrame) -> pd.DataFrame:
    date_column = "published_at" if "published_at" in frame.columns else "date"
    required = {"url", "title", "text", date_column}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Source is missing columns: {', '.join(missing)}")

    work = frame.copy()
    work["url"] = work["url"].fillna("").astype(str).map(_clean_text)
    work["title"] = work["title"].fillna("").astype(str).map(_clean_text)
    work["text"] = work["text"].fillna("").astype(str).map(_clean_text)
    work["published_at"] = pd.to_datetime(work[date_column], errors="coerce")

    topic = _text_column(work, "topic")
    tags = _text_column(work, "tags")
    work["topic"] = topic.mask(topic == "", tags).replace("", "Без темы")
    work["tags"] = tags

    if "news_id" in work.columns:
        news_ids = work["news_id"].fillna("").astype(str).str.strip()
    else:
        news_ids = pd.Series("", index=work.index, dtype=str)
    missing_ids = news_ids == ""
    news_ids.loc[missing_ids] = work.loc[missing_ids, "url"].map(_stable_news_id)
    work["news_id"] = news_ids

    valid = (
        (work["url"] != "")
        & (work["title"] != "")
        & (work["text"] != "")
        & work["published_at"].notna()
    )
    return work.loc[
        valid,
        ["news_id", "url", "title", "text", "topic", "tags", "published_at"],
    ]


def _text_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype=str)
    return frame[column].fillna("").astype(str).map(_clean_text)


def _stable_news_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def _clean_text(value: str) -> str:
    return "\n".join(line.rstrip() for line in value.splitlines()).strip()


def main() -> None:
    args = parse_args()
    selected = build_fixture(args.source, args.output, args.rows)
    dates = pd.to_datetime(selected["published_at"])
    print(
        f"Wrote {len(selected)} rows to {args.output} "
        f"({dates.min().date()} through {dates.max().date()}, "
        f"{selected['topic'].nunique()} topics)"
    )


if __name__ == "__main__":
    main()
