from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import requests

try:
    from config import NEWS_IMPORT_UPLOAD_TIMEOUT_SECONDS
except ModuleNotFoundError:
    from ui.config import NEWS_IMPORT_UPLOAD_TIMEOUT_SECONDS

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
    "Organization access has expired": "Срок доступа организации истек",
    "Authorization bearer token is required": "Требуется токен авторизации",
    "Invalid access token": "Недействительный токен авторизации",
    "User account is no longer available": "Учётная запись пользователя недоступна",
    "Admin role is required": "Требуется роль администратора",
    "Publisher role is required": "Требуется роль редактора",
}


class ApiError(RuntimeError):
    """Ошибка HTTP API с уже подготовленным для UI сообщением."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        """Сохранить человекочитаемое сообщение и HTTP-статус ответа."""
        super().__init__(message)
        self.status_code = status_code


def is_authentication_error(error: ApiError) -> bool:
    """Проверить, что ошибка API означает недействительную пользовательскую сессию."""
    return error.status_code in {401, 403} or str(error) == ERROR_TRANSLATIONS[
        "User account is no longer available"
    ]


class ApiClient:
    """Синхронный клиент Streamlit-интерфейса для публичного FastAPI API."""

    def __init__(self, base_url: str, token: str | None = None) -> None:
        """Создать клиент с базовым URL API и необязательным bearer token."""
        self.base_url = base_url.rstrip("/")
        self.token = token

    def set_token(self, token: str) -> None:
        """Установить bearer token для последующих запросов."""
        self.token = token

    def logout(self) -> None:
        """Очистить bearer token без обращения к серверу."""
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
        """Получить access token по логину и паролю."""
        data = self._request(
            "POST",
            "/auth/login",
            json={"login": login, "password": password},
        )
        return data["access_token"]

    def create_user(self, login: str, password: str) -> dict:
        """Создать обычного пользователя через публичный endpoint регистрации."""
        return self._request(
            "POST",
            "/users",
            json={"login": login, "password": password},
        )

    def get_me(self) -> dict:
        """Загрузить профиль текущего пользователя."""
        return self._request("GET", "/users/me")

    def list_users(self, role: str | None = None) -> list[dict]:
        """Вернуть список пользователей, при необходимости отфильтрованный по роли."""
        params = {"role": role} if role else None
        return self._request("GET", "/users", params=params)

    def create_admin_user(
        self,
        login: str,
        password: str,
        role: str,
        organization_id: str,
    ) -> dict:
        """Создать пользователя от имени администратора."""
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
        """Изменить роль пользователя через административный endpoint."""
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
        """Обновить основные поля пользователя: логин, роль и организацию."""
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
        """Удалить пользователя по идентификатору."""
        self._request("DELETE", f"/users/{user_id}")

    def list_organizations(self) -> list[dict]:
        """Вернуть организации с краткой административной сводкой."""
        return self._request("GET", "/organizations")

    def create_organization(self, name: str) -> dict:
        """Создать новую организацию."""
        return self._request("POST", "/organizations", json={"name": name})

    def update_organization(
        self,
        organization_id: str,
        name: str,
        *,
        license_type: str,
        access_expires_at: str,
    ) -> dict:
        """Обновить название и лицензионные параметры организации."""
        return self._request(
            "PATCH",
            f"/organizations/{organization_id}",
            json={
                "name": name,
                "license_type": license_type,
                "access_expires_at": access_expires_at,
            },
        )

    def list_admin_audit(self, action: str | None = None) -> list[dict]:
        """Загрузить последние записи административного аудита."""
        params = {"action": action, "limit": 500} if action else {"limit": 500}
        return self._request("GET", "/admin/audit", params=params)

    def get_balance(self) -> dict:
        """Получить баланс организации текущего пользователя."""
        return self._request("GET", "/accounting/me/balance")

    def list_transactions(
        self,
        reason: str | None = None,
        *,
        admin: bool = False,
    ) -> list[dict]:
        """Вернуть историю операций баланса для пользователя или администратора."""
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
        """Пополнить баланс организации."""
        return self._request(
            "POST",
            "/accounting/credits",
            json={"organization_id": organization_id, "amount": str(amount)},
        )

    def adjust_credit(self, organization_id: str, amount: Decimal) -> dict:
        """Выполнить административную корректировку баланса организации."""
        return self._request(
            "POST",
            "/accounting/adjustments",
            json={"organization_id": organization_id, "amount": str(amount)},
        )

    def add_news(self, payload: dict) -> dict:
        """Создать одну публикацию через форму UI."""
        return self._request("POST", "/news", json=payload, timeout=60)

    def list_news_import_formats(self) -> list[dict]:
        """Получить список поддерживаемых форматов импорта новостей."""
        return self._request("GET", "/news/import-formats")

    def import_news(
        self,
        format_id: str,
        file_name: str,
        content: bytes,
        *,
        publish_immediately: bool = False,
    ) -> dict:
        """Синхронно импортировать файл с публикациями."""
        return self._request(
            "POST",
            "/news/import",
            data={
                "format": format_id,
                "publish_immediately": str(publish_immediately).lower(),
            },
            files={"file": (file_name, content, _news_import_content_type(file_name))},
            timeout=NEWS_IMPORT_UPLOAD_TIMEOUT_SECONDS,
        )

    def create_news_import_job(
        self,
        format_id: str,
        file_name: str,
        content: bytes,
        *,
        publish_immediately: bool = False,
    ) -> dict:
        """Создать асинхронную задачу импорта файла с публикациями."""
        return self._request(
            "POST",
            "/news/import-jobs",
            data={
                "format": format_id,
                "publish_immediately": str(publish_immediately).lower(),
            },
            files={"file": (file_name, content, _news_import_content_type(file_name))},
            timeout=NEWS_IMPORT_UPLOAD_TIMEOUT_SECONDS,
        )

    def get_news_import_job(self, import_job_id: str) -> dict:
        """Получить состояние задачи импорта новостей."""
        return self._request(
            "GET",
            f"/news/import-jobs/{import_job_id}",
            timeout=30,
        )

    def get_latest_news_import_job(self) -> dict:
        """Получить последнюю задачу импорта текущего пользователя."""
        return self._request(
            "GET",
            "/news/import-jobs/latest",
            timeout=30,
        )

    def publish_news(self, article_id: str) -> dict:
        """Опубликовать один черновик и поставить его в обработку."""
        return self._request("POST", f"/news/{article_id}/publish", timeout=60)

    def publish_news_batch(self, article_ids: list[str]) -> dict:
        """Опубликовать пачку публикаций и создать pipeline job."""
        return self._request(
            "POST",
            "/news/publish",
            json={"article_ids": article_ids},
            timeout=120,
        )

    def delete_news_drafts(self, article_ids: list[str]) -> dict:
        """Удалить выбранные черновики пользователя."""
        return self._request(
            "DELETE",
            "/news",
            json={"article_ids": article_ids},
        )

    def archive_news(self, article_ids: list[str]) -> dict:
        """Архивировать выбранные публикации."""
        return self._request(
            "POST",
            "/news/archive",
            json={"article_ids": article_ids},
        )

    def restore_news(self, article_ids: list[str]) -> dict:
        """Вернуть выбранные публикации из архива."""
        return self._request(
            "POST",
            "/news/restore",
            json={"article_ids": article_ids},
        )

    def update_news_novelty_labels(self, updates: list[dict]) -> dict:
        """Сохранить ручные правки novelty-label для обработанных публикаций."""
        return self._request(
            "POST",
            "/news/moderation-labels",
            json={"updates": updates},
        )

    def reprocess_news(self, article_ids: list[str]) -> dict:
        """Поставить выбранные публикации на повторную ML-обработку."""
        return self._request(
            "POST",
            "/news/reprocess",
            json={"article_ids": article_ids},
            timeout=120,
        )

    def list_news_history(self, page_size: int = 500) -> list[dict]:
        """Загрузить всю историю публикаций текущего пользователя постранично."""
        page_size = max(1, min(page_size, 500))
        history: list[dict] = []
        offset = 0
        while True:
            page = self._request(
                "GET",
                "/news/me/history",
                params={"limit": page_size, "offset": offset},
            )
            history.extend(page)
            if len(page) < page_size:
                return history
            offset += page_size

    def list_news_history_page(
        self,
        *,
        visibility: str,
        statuses: list[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Загрузить одну страницу истории публикаций с фильтрами UI."""
        params: dict[str, Any] = {
            "visibility": visibility,
            "limit": limit,
            "offset": offset,
        }
        if statuses:
            params["status"] = statuses
        return self._request(
            "GET",
            "/news/me/history",
            params=params,
        )

    def get_news_history_summary(self) -> dict:
        """Получить агрегированные счётчики истории публикаций."""
        return self._request("GET", "/news/me/history-summary")

    def list_news_feed(
        self,
        *,
        published_from: str,
        published_to: str,
        limit: int = 50,
        offset: int = 0,
        include_cluster_context: bool = False,
    ) -> dict:
        """Загрузить кластеризованную ленту публикаций за период."""
        return self._request(
            "GET",
            "/news/feed",
            params={
                "published_from": published_from,
                "published_to": published_to,
                "limit": limit,
                "offset": offset,
                "include_cluster_context": include_cluster_context,
            },
        )

    def get_adjacent_news_dates(
        self,
        *,
        published_from: str,
        published_to: str,
    ) -> dict:
        """Получить ближайшие даты с публикациями вокруг текущего периода."""
        return self._request(
            "GET",
            "/news/feed/adjacent-dates",
            params={
                "published_from": published_from,
                "published_to": published_to,
            },
        )

    def get_latest_news_date(self) -> dict:
        """Получить последнюю дату public-публикации."""
        return self._request("GET", "/news/feed/latest-date")

    def search_news(self, payload: dict) -> dict:
        """Создать семантический поисковый запрос по обработанным публикациям."""
        return self._request("POST", "/news-search", json=payload, timeout=60)

    def list_search_history(self) -> list[dict]:
        """Загрузить историю поисковых запросов текущего пользователя."""
        return self._request("GET", "/news-search/history")


def _news_import_content_type(file_name: str) -> str:
    suffixes = [suffix.lower() for suffix in Path(file_name).suffixes]
    if suffixes[-1:] == [".zip"]:
        return "application/zip"
    if suffixes[-1:] == [".bz2"]:
        return "application/x-bzip2"
    return "text/csv"
