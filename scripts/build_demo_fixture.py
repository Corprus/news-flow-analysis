from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_SOURCE = Path("data/prepared/lenta_golden_candidate_set_clustered.csv")
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the deterministic Lenta.ru corpus used by demo mode."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rows", type=int, default=250)
    return parser.parse_args()


def build_fixture(source: Path, output: Path, rows: int) -> pd.DataFrame:
    if rows < 1:
        raise ValueError("rows must be positive")
    frame = pd.read_csv(source)
    required = {
        "news_id",
        "url",
        "title",
        "text",
        "topic",
        "tags",
        "published_at",
        "cluster_size",
        "preliminary_cluster_id",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Source is missing columns: {', '.join(missing)}")

    frame = frame.sort_values(
        ["published_at", "preliminary_cluster_id", "news_id"],
        kind="stable",
    )
    related = frame[frame["cluster_size"] >= 2]
    related_target = min(rows * 3 // 4, len(related))
    selected_related = related.head(related_target)

    selected_ids = set(selected_related["news_id"])
    singleton_pool = frame[
        (frame["cluster_size"] == 1) & ~frame["news_id"].isin(selected_ids)
    ]
    remaining = rows - len(selected_related)
    if remaining > 0:
        singleton_indexes = _evenly_spaced_indexes(len(singleton_pool), remaining)
        selected_singletons = singleton_pool.iloc[singleton_indexes]
        selected = pd.concat([selected_related, selected_singletons], ignore_index=True)
    else:
        selected = selected_related

    selected = (
        selected.drop_duplicates("news_id")
        .sort_values(["published_at", "news_id"], kind="stable")
        .head(rows)
        .copy()
    )
    if len(selected) != rows:
        raise ValueError(f"Could select only {len(selected)} of {rows} requested rows")

    selected["language"] = "ru"
    output.parent.mkdir(parents=True, exist_ok=True)
    selected[OUTPUT_COLUMNS].to_csv(output, index=False)
    return selected


def _evenly_spaced_indexes(pool_size: int, count: int) -> list[int]:
    if count > pool_size:
        raise ValueError(f"Requested {count} rows from a pool of {pool_size}")
    if count == 1:
        return [0]
    return [round(index * (pool_size - 1) / (count - 1)) for index in range(count)]


def main() -> None:
    args = parse_args()
    selected = build_fixture(args.source, args.output, args.rows)
    related_count = int((selected["cluster_size"] >= 2).sum())
    cluster_count = selected["preliminary_cluster_id"].nunique()
    print(
        f"Wrote {len(selected)} rows to {args.output} "
        f"({related_count} related rows, {cluster_count} source clusters)"
    )


if __name__ == "__main__":
    main()
