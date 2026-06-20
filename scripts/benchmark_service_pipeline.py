from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

DEFAULT_INPUT = Path("data/prepared/lenta_clean_news.csv")
DEFAULT_REFERENCE = Path(
    "data/artifacts/final_pipeline_benchmark/final_predictions_10000.csv"
)
DEFAULT_OUTPUT = Path(
    "data/artifacts/service_runtime_benchmark/service_benchmark_10000.json"
)
IMPORT_COLUMNS = [
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
        description="Benchmark the complete API/RabbitMQ/model-service pipeline."
    )
    parser.add_argument("--base-url", default="http://localhost/api")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--reset-news", action="store_true")
    return parser.parse_args()


class ApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.trust_env = False

    def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        **kwargs: Any,
    ) -> Any:
        headers = kwargs.pop("headers", {})
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        response = self.session.request(
            method,
            f"{self.base_url}{path}",
            headers=headers,
            timeout=kwargs.pop("timeout", 30),
            **kwargs,
        )
        response.raise_for_status()
        return response.json() if response.content else None

    def login(self, login: str, password: str) -> str:
        response = self.request(
            "POST",
            "/v1/auth/login",
            json={"login": login, "password": password},
        )
        return str(response["access_token"])


def build_exact_fixture(
    *,
    input_path: Path,
    reference_path: Path,
    output_path: Path,
    chunk_size: int,
) -> int:
    reference = pd.read_csv(reference_path, usecols=["news_id"])
    reference_ids = reference["news_id"].astype(str).tolist()
    wanted = set(reference_ids)
    found: list[pd.DataFrame] = []

    usecols = ["news_id", "url", "title", "text", "topic", "tags", "published_at"]
    for chunk in pd.read_csv(
        input_path,
        usecols=usecols,
        chunksize=chunk_size,
    ):
        chunk["news_id"] = chunk["news_id"].astype(str)
        selected = chunk[chunk["news_id"].isin(wanted)]
        if not selected.empty:
            found.append(selected)

    if not found:
        raise RuntimeError("No benchmark rows were found in the source corpus")
    selected = pd.concat(found, ignore_index=True).drop_duplicates("news_id")
    selected = selected.set_index("news_id").reindex(reference_ids).reset_index()
    missing = selected[selected["title"].isna()]["news_id"].tolist()
    if missing:
        raise RuntimeError(f"Missing benchmark news IDs: {missing[:10]}")
    selected["language"] = "ru"
    selected[IMPORT_COLUMNS].to_csv(output_path, index=False)
    return len(selected)


def reset_news_tables() -> None:
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
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        (
            "TRUNCATE TABLE news_articles, news_search_queries, "
            "news_pipeline_jobs CASCADE;"
        ),
    ]
    subprocess.run(command, check=True, env=os.environ.copy())


def wait_for_job(
    client: ApiClient,
    job_id: str,
    timeout: int,
) -> tuple[dict[str, Any], float]:
    started = time.perf_counter()
    deadline = started + timeout
    while time.perf_counter() < deadline:
        job = client.request("GET", f"/v1/news-pipeline/{job_id}")
        if job["status"] in {"done", "failed"}:
            return job, time.perf_counter() - started
        time.sleep(0.5)
    raise TimeoutError(f"Pipeline job {job_id} did not finish in {timeout} seconds")


def main() -> None:
    args = parse_args()
    client = ApiClient(args.base_url)
    health = client.request("GET", "/health")
    if health.get("status") != "ok":
        raise RuntimeError(f"API is not healthy: {health}")

    if args.reset_news:
        reset_news_tables()

    with tempfile.TemporaryDirectory(prefix="news-flow-service-benchmark-") as temp_dir:
        fixture_path = Path(temp_dir) / "lenta_benchmark_10000.csv"
        fixture_started = time.perf_counter()
        rows = build_exact_fixture(
            input_path=args.input,
            reference_path=args.reference,
            output_path=fixture_path,
            chunk_size=args.chunk_size,
        )
        fixture_seconds = time.perf_counter() - fixture_started
        fixture_bytes = fixture_path.stat().st_size
        print(
            f"Built {rows} rows, {fixture_bytes / (1024 * 1024):.2f} MiB "
            f"in {fixture_seconds:.2f}s",
            flush=True,
        )

        admin_token = client.login("admin", "admin12345")
        publisher_token = client.login("demo", "demo12345")
        publisher = client.request("GET", "/v1/users/me", token=publisher_token)
        client.request(
            "POST",
            "/v1/accounting/credits",
            token=admin_token,
            json={
                "organization_id": publisher["organization_id"],
                "amount": "20000.00",
            },
        )

        import_started_at = datetime.now().astimezone()
        import_started = time.perf_counter()
        with fixture_path.open("rb") as fixture:
            imported = client.request(
                "POST",
                "/v1/news/import",
                token=publisher_token,
                data={
                    "format": "lenta",
                    "publish_immediately": "true",
                },
                files={"file": (fixture_path.name, fixture, "text/csv")},
                timeout=max(args.timeout, 600),
            )
        import_seconds = time.perf_counter() - import_started

        job_id = str(imported["job_id"])
        job, job_wall_seconds = wait_for_job(client, job_id, args.timeout)
        result = job.get("result") or {}
        result_summary = {
            "mode": result.get("mode"),
            "context_count": result.get("context_count"),
            "requested_count": len(result.get("requested_ids") or []),
            "updated_count": len(result.get("updated_ids") or []),
            "diagnostics": result.get("diagnostics"),
            "versions": result.get("versions"),
        }
        server_seconds = (
            datetime.fromisoformat(job["updated_at"])
            - datetime.fromisoformat(job["created_at"])
        ).total_seconds()

    payload = {
        "measured_at": datetime.now().astimezone().isoformat(),
        "environment": {
            "base_url": args.base_url,
            "pipeline_device": os.getenv("PIPELINE_DEVICE", "cuda"),
            "input_path": str(args.input),
            "reference_path": str(args.reference),
        },
        "fixture": {
            "rows": rows,
            "bytes": fixture_bytes,
            "build_seconds": fixture_seconds,
        },
        "import": {
            "started_at": import_started_at.isoformat(),
            "wall_seconds": import_seconds,
            "created_count": imported["created_count"],
            "duplicate_count": imported["duplicate_count"],
            "published_count": imported["published_count"],
        },
        "pipeline": {
            "job_id": job_id,
            "status": job["status"],
            "server_seconds": server_seconds,
            "wall_seconds_after_import_response": job_wall_seconds,
            "rows_per_second_server": rows / server_seconds,
            "rows_per_second_wall": rows / job_wall_seconds,
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "result": result_summary,
        },
        "total_import_and_pipeline_seconds": import_seconds + job_wall_seconds,
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
