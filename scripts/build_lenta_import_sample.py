from __future__ import annotations

import argparse
import bz2
import csv
import io
import zipfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a ZIP Lenta import sample from the raw CSV.bz2 dataset.",
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--skip-valid", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rows < 1:
        raise SystemExit("--rows must be positive")
    if args.skip_valid < 0:
        raise SystemExit("--skip-valid must be >= 0")

    args.target.parent.mkdir(parents=True, exist_ok=True)
    inner_name = args.target.with_suffix(".csv").name
    source_rows_seen = 0
    valid_rows_skipped = 0
    invalid_rows_skipped = 0
    written = 0

    with (
        bz2.open(args.source, "rt", encoding="utf-8-sig", newline="") as source,
        zipfile.ZipFile(
            args.target,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as archive,
    ):
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise SystemExit("source has no CSV header")
        with archive.open(inner_name, "w") as raw_target:
            target = io.TextIOWrapper(raw_target, encoding="utf-8", newline="")
            writer = csv.DictWriter(
                target,
                fieldnames=reader.fieldnames,
                lineterminator="\n",
            )
            writer.writeheader()
            for row in reader:
                source_rows_seen += 1
                if not _is_valid_import_row(row):
                    invalid_rows_skipped += 1
                    continue
                if valid_rows_skipped < args.skip_valid:
                    valid_rows_skipped += 1
                    continue
                writer.writerow(row)
                written += 1
                if written >= args.rows:
                    break
            target.flush()

    if written != args.rows:
        raise SystemExit(f"only wrote {written} rows, expected {args.rows}")

    print(f"target={args.target}")
    print(f"inner_name={inner_name}")
    print(f"source_rows_seen={source_rows_seen}")
    print(f"valid_rows_skipped={valid_rows_skipped}")
    print(f"invalid_rows_skipped={invalid_rows_skipped}")
    print(f"valid_rows_written={written}")
    print(f"target_bytes={args.target.stat().st_size}")


def _is_valid_import_row(row: dict[str, str | None]) -> bool:
    title = (row.get("title") or "").strip()
    text = (row.get("text") or "").strip()
    published_at = (row.get("published_at") or row.get("date") or "").strip()
    return bool(title and text and published_at)


if __name__ == "__main__":
    main()
