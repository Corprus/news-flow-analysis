import asyncio
from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from api.demo import (
    DemoPipelineBatch,
    DemoSeedResult,
    _split_demo_articles,
    validate_demo_settings,
)
from api.main import enqueue_demo_pipeline_jobs
from settings import Settings
from users.models import Organization, User, UserRole
from users.passwords import PasswordHasher
from users.service import UserService


class _ImportedNewsStub:
    def __init__(self, external_id: str) -> None:
        self.external_id = external_id


def make_settings(**overrides) -> Settings:
    values = {
        "POSTGRES_PASSWORD": "postgres-secret",
        "RABBITMQ_PASSWORD": "rabbit-secret",
        **overrides,
    }
    return Settings(**values)


@pytest.mark.parametrize("app_env", ["prod", "production", "PRODUCTION"])
def test_demo_mode_is_disabled_in_production(app_env: str) -> None:
    settings = make_settings(APP_ENV=app_env, DEMO_MODE=True)

    with pytest.raises(RuntimeError, match="disabled in production"):
        validate_demo_settings(settings)


def test_demo_defaults_are_usable_locally() -> None:
    settings = make_settings(DEMO_MODE=True)

    validate_demo_settings(settings)

    assert settings.demo_user_login == "demo"
    assert settings.demo_initial_credit == Decimal("100.00")


def test_user_service_can_create_multiple_users_in_one_organization() -> None:
    engine = create_engine("sqlite:///:memory:")
    Organization.__table__.create(engine)
    User.__table__.create(engine)

    with Session(engine) as session:
        organization = Organization(name="Shared organization")
        session.add(organization)
        session.flush()
        organization_id = UUID(organization.id)
        users = UserService(session, PasswordHasher("test-secret"))

        publisher = users.create_user(
            "publisher",
            "publisher-password",
            UserRole.PUBLISHER,
            organization_id=organization_id,
        )
        analyst = users.create_user(
            "analyst",
            "analyst-password",
            organization_id=organization_id,
        )

        assert publisher.organization_id == analyst.organization_id
        assert publisher.organization_id == str(organization_id)
        assert publisher.role == UserRole.PUBLISHER.value


class _JobRepositorySpy:
    def __init__(self) -> None:
        self.queued = []

    async def mark_queued(self, job_id: str, payload: dict) -> None:
        self.queued.append((job_id, payload))


class _PublisherSpy:
    def __init__(self) -> None:
        self.messages = []

    async def publish(self, message: dict) -> None:
        self.messages.append(message)


def test_demo_pipeline_jobs_are_queued_per_organization() -> None:
    repository = _JobRepositorySpy()
    publisher = _PublisherSpy()
    demo = DemoSeedResult(
        organization_id="10000000-0000-0000-0000-000000000001",
        article_ids_to_process=["00000000-0000-0000-0000-000000000001"],
        pipeline_batches=[
            DemoPipelineBatch(
                organization_id="10000000-0000-0000-0000-000000000001",
                article_ids_to_process=["00000000-0000-0000-0000-000000000001"],
            ),
            DemoPipelineBatch(
                organization_id="10000000-0000-0000-0000-000000000002",
                article_ids_to_process=["00000000-0000-0000-0000-000000000002"],
            ),
            DemoPipelineBatch(
                organization_id="10000000-0000-0000-0000-000000000003",
                article_ids_to_process=[],
            ),
        ],
        imported_article_count=2,
    )

    asyncio.run(enqueue_demo_pipeline_jobs(repository, publisher, demo))

    assert [payload for _, payload in repository.queued] == [
        {
            "news_ids": ["00000000-0000-0000-0000-000000000001"],
            "organization_id": "10000000-0000-0000-0000-000000000001",
            "mode": "incremental",
        },
        {
            "news_ids": ["00000000-0000-0000-0000-000000000002"],
            "organization_id": "10000000-0000-0000-0000-000000000002",
            "mode": "incremental",
        },
    ]
    assert [message["payload"] for message in publisher.messages] == [
        payload for _, payload in repository.queued
    ]


def test_demo_articles_are_split_between_organizations() -> None:
    articles = [_ImportedNewsStub(str(index)) for index in range(6)]

    primary, partner = _split_demo_articles(articles)  # type: ignore[arg-type]

    assert [article.external_id for article in primary] == ["0", "1", "2"]
    assert [article.external_id for article in partner] == ["3", "4", "5"]
    assert set(primary).isdisjoint(partner)
