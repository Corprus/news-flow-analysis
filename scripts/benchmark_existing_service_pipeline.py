from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

DEFAULT_OUTPUT = Path(
    "data/artifacts/service_runtime_benchmark/service_full_10000.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark a pipeline job for articles already stored in PostgreSQL."
    )
    parser.add_argument("--base-url", default="http://localhost/api")
    parser.add_argument("--mode", choices=("full", "incremental"), default="full")
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def load_article_ids(limit: int) -> list[str]:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        os.getenv("POSTGRES_USER", "news_flow"),
        "-d",
        os.getenv("POSTGRES_DB", "news_flow"),
        "-At",
        "-c",
        (
            "SELECT id FROM news_articles "
            "WHERE visibility = 'public' "
            f"ORDER BY published_at, id LIMIT {limit};"
        ),
    ]
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: int,
    **kwargs: Any,
) -> dict[str, Any]:
    response = session.request(method, url, timeout=timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def main() -> None:
    args = parse_args()
    session = requests.Session()
    session.trust_env = False
    base_url = args.base_url.rstrip("/")

    article_ids = load_article_ids(args.limit)
    if len(article_ids) != args.limit:
        raise RuntimeError(f"Expected {args.limit} public articles, got {len(article_ids)}")

    started_at = datetime.now().astimezone()
    started = time.perf_counter()
    created = request_json(
        session,
        "POST",
        f"{base_url}/v1/news-pipeline",
        timeout=30,
        json={"news_ids": article_ids, "mode": args.mode},
    )
    job_id = str(created["job_id"])
    deadline = started + args.timeout
    while time.perf_counter() < deadline:
        job = request_json(
            session,
            "GET",
            f"{base_url}/v1/news-pipeline/{job_id}",
            timeout=30,
        )
        if job["status"] in {"done", "failed"}:
            break
        time.sleep(0.5)
    else:
        raise TimeoutError(f"Pipeline job {job_id} exceeded {args.timeout} seconds")

    wall_seconds = time.perf_counter() - started
    server_seconds = (
        datetime.fromisoformat(job["updated_at"])
        - datetime.fromisoformat(job["created_at"])
    ).total_seconds()
    result = job.get("result") or {}
    payload = {
        "measured_at": datetime.now().astimezone().isoformat(),
        "started_at": started_at.isoformat(),
        "mode": args.mode,
        "rows": len(article_ids),
        "job_id": job_id,
        "status": job["status"],
        "server_seconds": server_seconds,
        "wall_seconds": wall_seconds,
        "rows_per_second_server": len(article_ids) / server_seconds,
        "rows_per_second_wall": len(article_ids) / wall_seconds,
        "result": {
            "mode": result.get("mode"),
            "requested_count": len(result.get("requested_ids") or []),
            "updated_count": len(result.get("updated_ids") or []),
            "context_count": result.get("context_count"),
            "diagnostics": result.get("diagnostics"),
            "versions": result.get("versions"),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if job["status"] != "done":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
