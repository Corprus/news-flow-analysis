import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import HTTPException

from news.routes import AddNewsRequest, add_news, get_news_import_formats, publish_news
from users.deps import CurrentUser, ensure_publisher
from users.models import UserRole


def _current_user(role: UserRole) -> CurrentUser:
    return CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=role,
    )


def test_regular_user_cannot_publish_news() -> None:
    with pytest.raises(HTTPException) as error:
        ensure_publisher(_current_user(UserRole.USER))

    assert error.value.status_code == 403
    assert error.value.detail == "Publisher role is required"


@pytest.mark.parametrize("role", [UserRole.PUBLISHER, UserRole.ADMIN])
def test_publisher_and_admin_can_publish_news(role: UserRole) -> None:
    ensure_publisher(_current_user(role))


class _NewsServiceSpy:
    def __init__(self) -> None:
        self.add_calls = []

    def add_user_article(self, **kwargs):
        self.add_calls.append(kwargs)
        raise AssertionError("regular user must be rejected before creating a draft")


def test_add_news_endpoint_rejects_regular_user_before_side_effects() -> None:
    news = _NewsServiceSpy()
    request = AddNewsRequest(
        title="Title",
        content="Content",
        published_at=datetime.now(UTC),
    )

    with pytest.raises(HTTPException) as error:
        asyncio.run(
            add_news(
                request=request,
                current_user=_current_user(UserRole.USER),
                news=news,
                accounting=None,
                settings=None,
                publisher=None,
                repository=None,
            )
        )

    assert error.value.status_code == 403
    assert news.add_calls == []


def test_import_formats_endpoint_requires_publisher_role() -> None:
    with pytest.raises(HTTPException) as error:
        get_news_import_formats(_current_user(UserRole.USER))

    assert error.value.status_code == 403


def test_publish_news_endpoint_rejects_regular_user_before_side_effects() -> None:
    with pytest.raises(HTTPException) as error:
        asyncio.run(
            publish_news(
                article_id=uuid4(),
                current_user=_current_user(UserRole.USER),
                news=None,
                accounting=None,
                settings=None,
                publisher=None,
                repository=None,
            )
        )

    assert error.value.status_code == 403
