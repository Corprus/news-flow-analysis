from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Protocol

MAX_IMPORT_ROWS = 50_000
MAX_IMPORT_FILE_BYTES = 200 * 1024 * 1024


class NewsImportError(ValueError):
    pass


@dataclass(frozen=True)
class ImportedNews:
    title: str
    content: str
    published_at: datetime
    url: str | None = None
    topic: str | None = None
    language: str | None = None
    external_id: str | None = None
    metadata: dict[str, object] | None = None


@dataclass(frozen=True)
class NewsImportFormat:
    id: str
    label: str
    file_extensions: tuple[str, ...]
    media_types: tuple[str, ...]


class NewsFileImporter(Protocol):
    format: NewsImportFormat

    def parse(self, content: bytes) -> list[ImportedNews]: ...


class LentaCsvImporter:
    format = NewsImportFormat(
        id="lenta",
        label="Lenta.ru CSV",
        file_extensions=(".csv", ".zip"),
        media_types=(
            "text/csv",
            "application/csv",
            "application/vnd.ms-excel",
            "application/zip",
            "application/x-zip-compressed",
        ),
    )

    _required_columns = frozenset({"title", "text"})

    def parse(self, content: bytes) -> list[ImportedNews]:
        if _looks_like_zip(content):
            return self._parse_zip(content)
        return self._parse_csv(content)

    def _parse_zip(self, content: bytes) -> list[ImportedNews]:
        if not content:
            raise NewsImportError("Uploaded file is empty")
        articles: list[ImportedNews] = []
        csv_count = 0
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                for member in archive.infolist():
                    if member.is_dir() or not _is_csv_path(member.filename):
                        continue
                    csv_count += 1
                    with archive.open(member) as csv_file:
                        csv_articles = self._parse_csv(csv_file.read())
                    if len(articles) + len(csv_articles) > MAX_IMPORT_ROWS:
                        raise NewsImportError(
                            f"Archive contains more than {MAX_IMPORT_ROWS} news rows"
                        )
                    articles.extend(csv_articles)
        except zipfile.BadZipFile as exc:
            raise NewsImportError("ZIP archive is invalid") from exc

        if csv_count == 0:
            raise NewsImportError("ZIP archive does not contain CSV files")
        if not articles:
            raise NewsImportError("ZIP archive does not contain any news rows")
        return articles

    def _parse_csv(self, content: bytes) -> list[ImportedNews]:
        text = _decode_csv(content)
        reader = csv.DictReader(io.StringIO(text))
        columns = set(reader.fieldnames or ())
        missing = sorted(self._required_columns - columns)
        if "date" not in columns and "published_at" not in columns:
            missing.append("date or published_at")
        if missing:
            raise NewsImportError(
                "Lenta CSV is missing required columns: " + ", ".join(missing)
            )

        articles: list[ImportedNews] = []
        errors: list[str] = []
        for row_number, row in enumerate(reader, start=2):
            if len(articles) >= MAX_IMPORT_ROWS:
                raise NewsImportError(
                    f"File contains more than {MAX_IMPORT_ROWS} news rows"
                )
            try:
                articles.append(self._parse_row(row, row_number))
            except NewsImportError as exc:
                errors.append(str(exc))
                if len(errors) >= 20:
                    break

        if errors:
            raise NewsImportError("Invalid Lenta CSV rows: " + "; ".join(errors))
        if not articles:
            raise NewsImportError("Lenta CSV does not contain any news rows")
        return articles

    @staticmethod
    def _parse_row(row: dict[str, str | None], row_number: int) -> ImportedNews:
        title = _required_value(row, "title", row_number)
        content = _required_value(row, "text", row_number)
        date_value = _optional_value(row, "published_at") or _optional_value(row, "date")
        if date_value is None:
            raise NewsImportError(f"row {row_number}: published date is empty")
        published_at = _parse_datetime(date_value, row_number)
        tags = _optional_value(row, "tags")
        metadata = {"tags": tags} if tags is not None else None
        return ImportedNews(
            title=title,
            content=content,
            published_at=published_at,
            url=_optional_value(row, "url"),
            topic=_optional_value(row, "topic"),
            language=_optional_value(row, "language") or "ru",
            external_id=_optional_value(row, "news_id"),
            metadata=metadata,
        )


class NewsImporterRegistry:
    def __init__(self, importers: list[NewsFileImporter]) -> None:
        self._importers = {importer.format.id: importer for importer in importers}

    def list_formats(self) -> list[NewsImportFormat]:
        return [importer.format for importer in self._importers.values()]

    def parse(self, format_id: str, content: bytes) -> list[ImportedNews]:
        importer = self._importers.get(format_id)
        if importer is None:
            supported = ", ".join(self._importers) or "<none>"
            raise NewsImportError(
                f"Unsupported news import format {format_id!r}; supported: {supported}"
            )
        return importer.parse(content)


news_importers = NewsImporterRegistry([LentaCsvImporter()])


def _looks_like_zip(content: bytes) -> bool:
    if not content:
        return False
    return zipfile.is_zipfile(io.BytesIO(content))


def _is_csv_path(path: str) -> bool:
    return PurePosixPath(path).suffix.lower() == ".csv"


def _decode_csv(content: bytes) -> str:
    if not content:
        raise NewsImportError("Uploaded file is empty")
    for encoding in ("utf-8-sig", "cp1251"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise NewsImportError("CSV must use UTF-8 or Windows-1251 encoding")


def _required_value(
    row: dict[str, str | None],
    column: str,
    row_number: int,
) -> str:
    value = _optional_value(row, column)
    if value is None:
        raise NewsImportError(f"row {row_number}: {column} is empty")
    return value


def _optional_value(row: dict[str, str | None], column: str) -> str | None:
    value = row.get(column)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _parse_datetime(value: str, row_number: int) -> datetime:
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise NewsImportError(
            f"row {row_number}: invalid published date {value!r}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed
