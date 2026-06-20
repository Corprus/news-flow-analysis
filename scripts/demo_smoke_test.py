from __future__ import annotations

import argparse
import time
from typing import Any

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify demo users, organizations, pipeline, and semantic search."
    )
    parser.add_argument("--base-url", default="http://localhost/api")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument(
        "--query",
        default="международная политика и отношения России с другими странами",
    )
    return parser.parse_args()


class DemoClient:
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
            timeout=30,
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


def wait_for_search(
    client: DemoClient,
    token: str,
    query_id: str,
    timeout: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        history = client.request("GET", "/v1/news-search/history", token=token)
        query = next(
            (item for item in history if item["query_id"] == query_id),
            None,
        )
        if query is not None and query["status"] == "done":
            return query
        if query is not None and query["status"] == "failed":
            raise RuntimeError(f"Demo search failed: {query.get('error')}")
        time.sleep(5)
    raise TimeoutError(f"Demo search did not finish in {timeout} seconds")


def main() -> None:
    args = parse_args()
    client = DemoClient(args.base_url)

    demo_token = client.login("demo", "demo12345")
    analyst_token = client.login("analyst", "analyst12345")
    partner_token = client.login("partner_user", "partner12345")
    admin_token = client.login("admin", "admin12345")

    demo = client.request("GET", "/v1/users/me", token=demo_token)
    analyst = client.request("GET", "/v1/users/me", token=analyst_token)
    partner = client.request("GET", "/v1/users/me", token=partner_token)
    users = client.request("GET", "/v1/users", token=admin_token)

    if demo["organization_id"] != analyst["organization_id"]:
        raise RuntimeError("demo and analyst must belong to the same organization")
    if demo["organization_id"] == partner["organization_id"]:
        raise RuntimeError("partner_user must belong to another organization")
    if len(users) < 5:
        raise RuntimeError(f"Expected at least five demo users, got {len(users)}")

    operations = client.request(
        "GET",
        "/v1/accounting/me/transactions",
        token=demo_token,
    )
    batched_publications = [
        operation
        for operation in operations
        if operation["reason"] == "news_add" and operation.get("item_count", 1) > 1
    ]
    single_publications = [
        operation
        for operation in operations
        if operation["reason"] == "news_add" and operation.get("item_count", 1) == 1
    ]
    if len(batched_publications) != 1:
        raise RuntimeError(
            f"Expected one batched demo publication, got {len(batched_publications)}"
        )
    if len(single_publications) < 3:
        raise RuntimeError(
            f"Expected at least three single demo publications, got {len(single_publications)}"
        )

    created = client.request(
        "POST",
        "/v1/news-search",
        token=partner_token,
        json={
            "query_text": args.query,
            "top_k": 5,
            "language": "ru",
            "min_relevance": 0.0,
        },
    )
    completed = wait_for_search(
        client,
        partner_token,
        str(created["query_id"]),
        args.timeout,
    )
    result = completed.get("result") or {}
    clusters = result.get("clusters") or []
    if not clusters:
        raise RuntimeError("Demo semantic search returned no clusters")

    print(
        "Demo smoke test passed: "
        f"{len(users)} users, one publication batch, "
        f"{len(single_publications)} single publications, "
        f"cross-organization search returned {len(clusters)} clusters."
    )
    for cluster in clusters[:3]:
        print(f"- {cluster['score']:.3f}: {cluster['representative_title']}")


if __name__ == "__main__":
    main()
