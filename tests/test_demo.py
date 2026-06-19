from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from api.demo import validate_demo_settings
from settings import Settings
from users.models import Organization, User, UserRole
from users.passwords import PasswordHasher
from users.service import UserService


def make_settings(**overrides) -> Settings:
    values = {
        "POSTGRES_PASSWORD": "postgres-secret",
        "RABBITMQ_PASSWORD": "rabbit-secret",
        **overrides,
    }
    return Settings(**values)


def test_drop_db_requires_demo_mode() -> None:
    settings = make_settings(DEMO_DROP_DB=True)

    with pytest.raises(RuntimeError, match="requires DEMO_MODE"):
        validate_demo_settings(settings)


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
