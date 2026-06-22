from __future__ import annotations

from decimal import Decimal
from typing import Any

import requests

ERROR_TRANSLATIONS = {
    "Invalid login or password": "Неверный логин или пароль",
    "User already exists": "Пользователь с таким логином уже существует",
    "User not found": "Пользователь не найден",
    "Organization not found": "Организация не найдена",
    "Organization does not exist": "Организация не существует",
    "Organization already exists": "Организация с таким названием уже существует",
    "An administrator cannot remove their own admin role": (
        "Администратор не может снять роль администратора у самого себя"
    ),
    "An administrator cannot delete their own account": (
        "Администратор не может удалить собственную учётную запись"
    ),
    "The last administrator cannot be demoted": (
        "Нельзя снять роль у последнего администратора"
    ),
    "The last administrator cannot be deleted": (
        "Нельзя удалить последнего администратора"
    ),
    "Amount must not be zero": "Сумма корректировки не может быть нулевой",
    "Amount must be a whole number": "Можно указать только целое количество единиц",
    "Insufficient organization balance": (
        "Недостаточно средств на балансе организации"
    ),
    "Insufficient credits": "Недостаточно средств для публикации новостей",
    "Authorization bearer token is required": "Требуется токен авторизации",
    "Invalid access token": "Недействительный токен авторизации",
    "User account is no longer available": "Учётная запись пользователя недоступна",
    "Admin role is required": "Требуется роль администратора",
    "Publisher role is required": "Требуется роль редактора",
}


class ApiError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


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
            try:
                detail = response.json().get("detail", response.text)
            except (ValueError, AttributeError):
                detail = response.text
            raise ApiError(
                ERROR_TRANSLATIONS.get(str(detail), str(detail)),
                status_code=response.status_code,
            )
        if response.status_code == 204:
            return None
        return response.json()

    def login(self, login: str, password: str) -> str:
        data = self._request(
            "POST",
            "/auth/login",
            json={"login": login, "password": password},
        )
        return data["access_token"]

    def create_user(self, login: str, password: str) -> dict:
        return self._request(
            "POST",
            "/users",
            json={"login": login, "password": password},
        )

    def get_me(self) -> dict:
        return self._request("GET", "/users/me")

    def list_users(self, role: str | None = None) -> list[dict]:
        params = {"role": role} if role else None
        return self._request("GET", "/users", params=params)

    def create_admin_user(
        self,
        login: str,
        password: str,
        role: str,
        organization_id: str,
    ) -> dict:
        return self._request(
            "POST",
            "/admin/users",
            json={
                "login": login,
                "password": password,
                "role": role,
                "organization_id": organization_id,
            },
        )

    def update_user_role(self, user_id: str, role: str) -> dict:
        return self._request(
            "PATCH",
            f"/users/{user_id}/role",
            json={"role": role},
        )

    def update_user(
        self,
        user_id: str,
        *,
        login: str,
        role: str,
        organization_id: str,
    ) -> dict:
        return self._request(
            "PATCH",
            f"/users/{user_id}",
            json={
                "login": login,
                "role": role,
                "organization_id": organization_id,
            },
        )

    def delete_user(self, user_id: str) -> None:
        self._request("DELETE", f"/users/{user_id}")

    def list_organizations(self) -> list[dict]:
        return self._request("GET", "/organizations")

    def create_organization(self, name: str) -> dict:
        return self._request("POST", "/organizations", json={"name": name})

    def update_organization(self, organization_id: str, name: str) -> dict:
        return self._request(
            "PATCH",
            f"/organizations/{organization_id}",
            json={"name": name},
        )

    def list_admin_audit(self, action: str | None = None) -> list[dict]:
        params = {"action": action, "limit": 500} if action else {"limit": 500}
        return self._request("GET", "/admin/audit", params=params)

    def get_balance(self) -> dict:
        return self._request("GET", "/accounting/me/balance")

    def list_transactions(
        self,
        reason: str | None = None,
        *,
        admin: bool = False,
    ) -> list[dict]:
        params = {"limit": 500 if admin else 100}
        if reason:
            params["reason"] = reason
        path = (
            "/accounting/admin/transactions"
            if admin
            else "/accounting/me/transactions"
        )
        return self._request("GET", path, params=params)

    def add_credit(self, organization_id: str, amount: Decimal) -> dict:
        return self._request(
            "POST",
            "/accounting/credits",
            json={"organization_id": organization_id, "amount": str(amount)},
        )

    def adjust_credit(self, organization_id: str, amount: Decimal) -> dict:
        return self._request(
            "POST",
            "/accounting/adjustments",
            json={"organization_id": organization_id, "amount": str(amount)},
        )

    def add_news(self, payload: dict) -> dict:
        return self._request("POST", "/news", json=payload, timeout=60)

    def list_news_import_formats(self) -> list[dict]:
        return self._request("GET", "/news/import-formats")

    def import_news(
        self,
        format_id: str,
        file_name: str,
        content: bytes,
        *,
        publish_immediately: bool = False,
    ) -> dict:
        return self._request(
            "POST",
            "/news/import",
            data={
                "format": format_id,
                "publish_immediately": str(publish_immediately).lower(),
            },
            files={"file": (file_name, content, "text/csv")},
            timeout=120,
        )

    def publish_news(self, article_id: str) -> dict:
        return self._request("POST", f"/news/{article_id}/publish", timeout=60)

    def publish_news_batch(self, article_ids: list[str]) -> dict:
        return self._request(
            "POST",
            "/news/publish",
            json={"article_ids": article_ids},
            timeout=120,
        )

    def delete_news_drafts(self, article_ids: list[str]) -> dict:
        return self._request(
            "DELETE",
            "/news",
            json={"article_ids": article_ids},
        )

    def archive_news(self, article_ids: list[str]) -> dict:
        return self._request(
            "POST",
            "/news/archive",
            json={"article_ids": article_ids},
        )

    def restore_news(self, article_ids: list[str]) -> dict:
        return self._request(
            "POST",
            "/news/restore",
            json={"article_ids": article_ids},
        )

    def update_news_novelty_labels(self, updates: list[dict]) -> dict:
        return self._request(
            "POST",
            "/news/moderation-labels",
            json={"updates": updates},
        )

    def reprocess_news(self, article_ids: list[str]) -> dict:
        return self._request(
            "POST",
            "/news/reprocess",
            json={"article_ids": article_ids},
            timeout=120,
        )

    def list_news_history(self, limit: int = 10_000) -> list[dict]:
        return self._request(
            "GET",
            "/news/me/history",
            params={"limit": limit},
        )

    def list_news_feed(
        self,
        *,
        published_from: str,
        published_to: str,
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        return self._request(
            "GET",
            "/news/feed",
            params={
                "published_from": published_from,
                "published_to": published_to,
                "limit": limit,
                "offset": offset,
            },
        )

    def get_adjacent_news_dates(
        self,
        *,
        published_from: str,
        published_to: str,
    ) -> dict:
        return self._request(
            "GET",
            "/news/feed/adjacent-dates",
            params={
                "published_from": published_from,
                "published_to": published_to,
            },
        )

    def get_latest_news_date(self) -> dict:
        return self._request("GET", "/news/feed/latest-date")

    def search_news(self, payload: dict) -> dict:
        return self._request("POST", "/news-search", json=payload, timeout=60)

    def list_search_history(self) -> list[dict]:
        return self._request("GET", "/news-search/history")
