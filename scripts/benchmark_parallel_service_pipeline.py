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
    "data/artifacts/service_runtime_benchmark/service_cpu_parallel_10000.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark aggregate service throughput by splitting existing articles "
            "between concurrent full-pipeline jobs."
        )
    )
    parser.add_argument("--base-url", default="http://localhost/api")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--timeout", type=int, default=3600)
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
        os.getenv("POSTGRES_USER", "semantic_news_novelty"),
        "-d",
        os.getenv("POSTGRES_DB", "semantic_news_novelty"),
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


def split_contiguous(values: list[str], parts: int) -> list[list[str]]:
    if parts < 1:
        raise ValueError("workers must be at least 1")
    if parts > len(values):
        raise ValueError("workers cannot exceed the number of articles")
    quotient, remainder = divmod(len(values), parts)
    result: list[list[str]] = []
    offset = 0
    for index in range(parts):
        size = quotient + (1 if index < remainder else 0)
        result.append(values[offset : offset + size])
        offset += size
    return result


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
    article_ids = load_article_ids(args.limit)
    if len(article_ids) != args.limit:
        raise RuntimeError(f"Expected {args.limit} public articles, got {len(article_ids)}")
    partitions = split_contiguous(article_ids, args.workers)

    session = requests.Session()
    session.trust_env = False
    base_url = args.base_url.rstrip("/")
    health = request_json(session, "GET", f"{base_url}/health", timeout=30)
    if health.get("status") != "ok":
        raise RuntimeError(f"API is not healthy: {health}")

    started_at = datetime.now().astimezone()
    started = time.perf_counter()
    job_rows: dict[str, int] = {}
    for partition in partitions:
        created = request_json(
            session,
            "POST",
            f"{base_url}/news-pipeline",
            timeout=30,
            json={"news_ids": partition, "mode": "full"},
        )
        job_rows[str(created["job_id"])] = len(partition)

    pending = set(job_rows)
    completed_jobs: dict[str, dict[str, Any]] = {}
    deadline = started + args.timeout
    while pending and time.perf_counter() < deadline:
        for job_id in list(pending):
            job = request_json(
                session,
                "GET",
                f"{base_url}/news-pipeline/{job_id}",
                timeout=30,
            )
            if job["status"] in {"done", "failed"}:
                completed_jobs[job_id] = job
                pending.remove(job_id)
        if pending:
            time.sleep(0.5)
    if pending:
        raise TimeoutError(
            f"{len(pending)} pipeline jobs did not finish in {args.timeout} seconds"
        )

    wall_seconds = time.perf_counter() - started
    jobs: list[dict[str, Any]] = []
    for job_id, rows in job_rows.items():
        job = completed_jobs[job_id]
        server_seconds = (
            datetime.fromisoformat(job["updated_at"])
            - datetime.fromisoformat(job["created_at"])
        ).total_seconds()
        result = job.get("result") or {}
        jobs.append(
            {
                "job_id": job_id,
                "rows": rows,
                "status": job["status"],
                "server_seconds": server_seconds,
                "rows_per_second_server": rows / server_seconds,
                "created_at": job["created_at"],
                "updated_at": job["updated_at"],
                "result": {
                    "requested_count": len(result.get("requested_ids") or []),
                    "updated_count": len(result.get("updated_ids") or []),
                    "diagnostics": result.get("diagnostics"),
                    "versions": result.get("versions"),
                    "error": result.get("error"),
                },
            }
        )

    earliest_created = min(datetime.fromisoformat(job["created_at"]) for job in jobs)
    latest_updated = max(datetime.fromisoformat(job["updated_at"]) for job in jobs)
    server_window_seconds = (latest_updated - earliest_created).total_seconds()
    payload = {
        "measured_at": datetime.now().astimezone().isoformat(),
        "started_at": started_at.isoformat(),
        "mode": "full",
        "workers": args.workers,
        "rows": len(article_ids),
        "partition_rows": [len(partition) for partition in partitions],
        "wall_seconds": wall_seconds,
        "server_window_seconds": server_window_seconds,
        "rows_per_second_wall": len(article_ids) / wall_seconds,
        "rows_per_second_server_window": len(article_ids) / server_window_seconds,
        "all_jobs_done": all(job["status"] == "done" for job in jobs),
        "jobs": jobs,
        "methodology_note": (
            "With more than one worker the corpus is split into independent full jobs. "
            "This measures aggregate throughput, not globally consistent clustering "
            "across all articles."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not payload["all_jobs_done"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
