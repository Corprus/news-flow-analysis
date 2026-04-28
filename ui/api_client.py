from __future__ import annotations

from decimal import Decimal
from typing import Any

import requests


class ApiError(RuntimeError):
    pass


class ApiClient:
    def __init__(self, base_url: str, token: str | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def set_token(self, token: str) -> None:
        self.token = token

    def logout(self) -> None:
        self.token = None

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = requests.request(
            method,
            f"{self.base_url}{path}",
            headers={**self._headers(), **kwargs.pop("headers", {})},
            timeout=kwargs.pop("timeout", 30),
            **kwargs,
        )
        if not 200 <= response.status_code < 300:
            raise ApiError(f"{method} {path} failed: {response.status_code} {response.text}")
        if response.status_code == 204:
            return None
        return response.json()

    def login(self, login: str, password: str) -> str:
        data = self._request(
            "POST",
            "/v1/auth/login",
            json={"login": login, "password": password},
        )
        return data["access_token"]

    def create_user(self, login: str, password: str) -> dict:
        return self._request(
            "POST",
            "/v1/users",
            json={"login": login, "password": password},
        )

    def get_me(self) -> dict:
        return self._request("GET", "/v1/users/me")

    def list_users(self) -> list[dict]:
        return self._request("GET", "/v1/users")

    def get_balance(self) -> dict:
        return self._request("GET", "/v1/accounting/me/balance")

    def list_transactions(self, reason: str | None = None) -> list[dict]:
        params = {"reason": reason} if reason else None
        return self._request("GET", "/v1/accounting/me/transactions", params=params)

    def add_credit(self, user_id: str, amount: Decimal) -> dict:
        return self._request(
            "POST",
            "/v1/accounting/credits",
            json={"user_id": user_id, "amount": str(amount)},
        )

    def add_news(self, payload: dict) -> dict:
        return self._request("POST", "/v1/news", json=payload, timeout=60)

    def list_news_history(self) -> list[dict]:
        return self._request("GET", "/v1/news/me/history")

    def search_news(self, payload: dict) -> dict:
        return self._request("POST", "/v1/news-search", json=payload, timeout=60)

    def list_search_history(self) -> list[dict]:
        return self._request("GET", "/v1/news-search/history")
