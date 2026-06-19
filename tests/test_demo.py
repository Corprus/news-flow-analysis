from decimal import Decimal

import pytest

from api.demo import validate_demo_settings
from settings import Settings


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
