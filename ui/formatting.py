from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

DEMO_DUPLICATE_TITLE_RE = re.compile(r"\s*\(повтор(?:\s+[^)]*)?\)\s*$")


def parse_decimal(value: str) -> Decimal | None:
    try:
        amount = Decimal(value.strip())
    except (InvalidOperation, ValueError):
        return None
    return amount if amount > 0 else None


def format_amount(value: object) -> str:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value)
    if amount == amount.to_integral_value():
        return str(int(amount))
    return format(amount.normalize(), "f")


def format_search_result_summary(result: dict) -> str:
    clusters = result.get("clusters") or []
    if not clusters:
        return ""
    publication_count = sum(
        int(cluster.get("article_count", len(cluster.get("items", []))))
        for cluster in clusters
    )
    return (
        f'[🗂](# "Количество сюжетов") {len(clusters)}'
        f' · [📰](# "Количество публикаций") {publication_count}'
    )


def format_search_date(
    value: str | None,
    *,
    date_only: bool = False,
    hide_midnight: bool = False,
) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if hide_midnight and parsed.time().replace(tzinfo=None) == datetime.min.time():
            date_only = True
        return parsed.strftime("%d.%m.%Y" if date_only else "%d.%m.%Y %H:%M")
    except (TypeError, ValueError):
        return str(value)


def escape_markdown(value: str) -> str:
    for character in ("\\", "*", "_", "`", "[", "]"):
        value = value.replace(character, f"\\{character}")
    return value


def display_news_title(value: object, fallback: str = "Без названия") -> str:
    title = str(value or fallback).strip()
    return DEMO_DUPLICATE_TITLE_RE.sub("", title) or fallback


def display_news_url(value: object) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    parts = urlsplit(url)
    if not parts.query:
        return url
    query = [
        (key, current_value)
        for key, current_value in parse_qsl(parts.query, keep_blank_values=True)
        if key != "demo_duplicate"
    ]
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
    )
