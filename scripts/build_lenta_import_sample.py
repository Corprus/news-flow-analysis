from __future__ import annotations

import csv
import hashlib
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "prepared" / "lenta_clean_news.csv"
OUTPUT = ROOT / "data" / "import" / "lenta_import_sample_1000.csv"
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


def load_existing_keys() -> tuple[set[str], set[str], set[str]]:
    env = os.environ.copy()
    env.setdefault("POSTGRES_PASSWORD", "demo_postgres_change_me")
    env.setdefault("RABBITMQ_PASSWORD", "demo_rabbitmq_change_me")
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "news_flow",
        "-d",
        "news_flow",
        "-At",
        "-F",
        "\t",
        "-c",
        (
            "SELECT COALESCE(external_id,''), COALESCE(canonical_url,''), "
            "COALESCE(url,''), COALESCE(content_hash,'') FROM news_articles"
        ),
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    external_ids: set[str] = set()
    urls: set[str] = set()
    content_hashes: set[str] = set()
    for line in result.stdout.splitlines():
        external_id, canonical_url, url, content_hash = line.split("\t", 3)
        if external_id:
            external_ids.add(external_id)
        if canonical_url:
            urls.add(canonical_url)
        if url:
            urls.add(url)
        if content_hash:
            content_hashes.add(content_hash)
    return external_ids, urls, content_hashes


def main() -> None:
    external_ids, urls, content_hashes = load_existing_keys()
    selected: list[dict[str, str]] = []
    selected_ids: set[str] = set()
    selected_urls: set[str] = set()
    selected_hashes: set[str] = set()

    with SOURCE.open("r", encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            news_id = (row.get("news_id") or "").strip()
            url = (row.get("url") or "").strip()
            title = (row.get("title") or "").strip()
            text = (row.get("text") or "").strip()
            published_at = (row.get("published_at") or "").strip()
            content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
            if not news_id or not title or not text or not published_at:
                continue
            if (
                news_id in external_ids
                or news_id in selected_ids
                or url in urls
                or url in selected_urls
                or content_hash in content_hashes
                or content_hash in selected_hashes
            ):
                continue
            selected.append(
                {
                    "news_id": news_id,
                    "url": url,
                    "title": title,
                    "text": text,
                    "topic": (row.get("topic") or "").strip(),
                    "tags": (row.get("tags") or "").strip(),
                    "published_at": published_at,
                    "language": "ru",
                }
            )
            selected_ids.add(news_id)
            selected_urls.add(url)
            selected_hashes.add(content_hash)
            if len(selected) == 1000:
                break

    if len(selected) != 1000:
        raise RuntimeError(f"Expected 1000 unique rows, found {len(selected)}")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8-sig", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(selected)

    print(OUTPUT)
    print(f"rows={len(selected)}")
    print(f"first_news_id={selected[0]['news_id']}")
    print(f"last_news_id={selected[-1]['news_id']}")


if __name__ == "__main__":
    main()
