from __future__ import annotations

import argparse
import bz2
import csv
import io
import shutil
import urllib.request
import zipfile
from datetime import date, datetime
from pathlib import Path

DEFAULT_SOURCE_URL = (
    "https://github.com/yutkin/Lenta.Ru-News-Dataset/releases/download/v1.1/"
    "lenta-ru-news.csv.bz2"
)
DEFAULT_SOURCE = Path("data/raw/lenta-ru-news.csv.bz2")
DEFAULT_TARGET = Path("data/import/lenta_import_sample.zip")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a Lenta import sample ZIP from the raw CSV.bz2 dataset. "
            "The raw dataset is reused from cache if it already exists."
        ),
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--source-url", default=DEFAULT_SOURCE_URL)
    parser.add_argument("--target", type=Path, default=DEFAULT_TARGET)
    parser.add_argument("--rows", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--skip-valid", type=int, default=0)
    parser.add_argument("--date-from", type=_parse_date)
    parser.add_argument("--date-to", type=_parse_date)
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Fail if --source is missing instead of downloading it from --source-url.",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Write a plain CSV instead of a ZIP archive.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = _resolve_rows(args.rows, args.limit)
    if args.skip_valid < 0:
        raise SystemExit("--skip-valid must be >= 0")
    if args.date_from is not None and args.date_to is not None:
        if args.date_from > args.date_to:
            raise SystemExit("--date-from must be earlier than or equal to --date-to")

    _ensure_source(args.source, args.source_url, args.no_download)
    args.target.parent.mkdir(parents=True, exist_ok=True)
    inner_name = _inner_csv_name(args.target)
    source_rows_seen = 0
    valid_rows_skipped = 0
    invalid_rows_skipped = 0
    written = 0

    with bz2.open(args.source, "rt", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise SystemExit("source has no CSV header")

        with _open_target(args.target, inner_name, args.csv) as target:
            writer = csv.DictWriter(target, fieldnames=reader.fieldnames, lineterminator="\n")
            writer.writeheader()
            for row in reader:
                source_rows_seen += 1
                if not _is_valid_import_row(row, args.date_from, args.date_to):
                    invalid_rows_skipped += 1
                    continue
                if valid_rows_skipped < args.skip_valid:
                    valid_rows_skipped += 1
                    continue
                writer.writerow(row)
                written += 1
                if rows is not None and written >= rows:
                    break

    if rows is not None and written != rows:
        raise SystemExit(f"only wrote {written} rows, expected {rows}")

    print(f"target={args.target}")
    if not args.csv:
        print(f"inner_name={inner_name}")
    print(f"source_rows_seen={source_rows_seen}")
    print(f"valid_rows_skipped={valid_rows_skipped}")
    print(f"invalid_rows_skipped={invalid_rows_skipped}")
    print(f"valid_rows_written={written}")
    print(f"target_bytes={args.target.stat().st_size}")


def _resolve_rows(rows: int | None, limit: int | None) -> int | None:
    if rows is not None and limit is not None and rows != limit:
        raise SystemExit("--rows and --limit must match if both are set")
    resolved = rows if rows is not None else limit
    if resolved is not None and resolved < 1:
        raise SystemExit("--rows/--limit must be positive")
    return resolved


def _ensure_source(source: Path, source_url: str, no_download: bool) -> None:
    if source.is_file():
        return
    if no_download:
        raise SystemExit(f"source not found: {source}")
    source.parent.mkdir(parents=True, exist_ok=True)
    temporary = source.with_suffix(source.suffix + ".download")
    print(f"Downloading {source_url}")
    print(f"Target cache: {source}")
    with urllib.request.urlopen(source_url) as response:
        with temporary.open("wb") as target:
            shutil.copyfileobj(response, target)
    temporary.replace(source)


def _open_target(target: Path, inner_name: str, plain_csv: bool):
    if plain_csv:
        return target.open("w", encoding="utf-8", newline="")
    archive = zipfile.ZipFile(
        target,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    )
    raw_target = archive.open(inner_name, "w")
    text_target = io.TextIOWrapper(raw_target, encoding="utf-8", newline="")
    return _ZipTextTarget(archive, raw_target, text_target)


class _ZipTextTarget:
    def __init__(self, archive, raw_target, text_target) -> None:
        self._archive = archive
        self._raw_target = raw_target
        self._text_target = text_target

    def __enter__(self):
        return self._text_target

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._text_target.flush()
        self._text_target.close()
        self._archive.close()


def _inner_csv_name(target: Path) -> str:
    if target.suffix.lower() == ".zip":
        return target.with_suffix(".csv").name
    return f"{target.name}.csv"


def _is_valid_import_row(
    row: dict[str, str | None],
    date_from: date | None,
    date_to: date | None,
) -> bool:
    title = (row.get("title") or "").strip()
    text = (row.get("text") or "").strip()
    published_at = (row.get("published_at") or row.get("date") or "").strip()
    if not (title and text and published_at):
        return False
    published_date = _parse_row_date(published_at)
    if published_date is None:
        return False
    if date_from is not None and published_date < date_from:
        return False
    if date_to is not None and published_date > date_to:
        return False
    return True


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _parse_row_date(value: str) -> date | None:
    normalized = value.strip().replace("/", "-")
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        pass
    try:
        return datetime.strptime(normalized[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


if __name__ == "__main__":
    main()
