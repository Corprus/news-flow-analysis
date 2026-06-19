from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy.orm import Session

from accounting.service import AccountingService
from news.service import NewsService
from settings import Settings
from users.models import User, UserRole
from users.passwords import PasswordHasher
from users.service import UserService


def validate_demo_settings(settings: Settings) -> None:
    if settings.demo_drop_db and not settings.demo_mode:
        raise RuntimeError("DEMO_DROP_DB requires DEMO_MODE=true")
    if settings.demo_mode and settings.app_env.lower() in {"prod", "production"}:
        raise RuntimeError("Demo mode is disabled in production")


def seed_demo(session: Session, settings: Settings) -> None:
    password_hasher = PasswordHasher(settings.password_hash_secret)
    users = UserService(session, password_hasher)
    accounting = AccountingService(session)
    news = NewsService(session)

    demo_user = _get_or_create_user(
        users,
        settings.demo_user_login,
        settings.demo_user_password,
        UserRole.PUBLISHER,
        session,
        password_hasher,
    )
    _get_or_create_user(
        users,
        settings.demo_admin_login,
        settings.demo_admin_password,
        UserRole.ADMIN,
        session,
        password_hasher,
    )

    organization_id = UUID(demo_user.organization_id)
    user_id = UUID(demo_user.id)
    current_balance = accounting.get_balance(organization_id)
    if current_balance < settings.demo_initial_credit:
        accounting.add_credit(
            organization_id,
            user_id,
            settings.demo_initial_credit - current_balance,
        )

    published_at = datetime.now(UTC)
    demo_articles = (
        (
            "В России представлен новый промышленный робот",
            "Компания представила промышленного робота для автоматизации складов "
            "и производственных линий. Пилотные внедрения начнутся в этом году.",
            "https://demo.news.local/industrial-robot",
            "technology",
        ),
        (
            "Разработчик расширил пилот промышленного робота",
            "После первых испытаний разработчик сообщил о расширении пилотной "
            "программы и подключении ещё двух производственных площадок.",
            "https://demo.news.local/industrial-robot-pilot",
            "technology",
        ),
        (
            "Аналитики оценили рынок автоматизации складов",
            "Исследование показывает рост спроса на роботизацию складской логистики "
            "и программное обеспечение для управления оборудованием.",
            "https://demo.news.local/warehouse-automation-market",
            "business",
        ),
    )
    for index, (title, content, url, topic) in enumerate(demo_articles):
        news.add_user_article(
            user_id=user_id,
            title=title,
            content=content,
            published_at=published_at - timedelta(hours=index),
            url=url,
            canonical_url=url,
            language="ru",
            topic=topic,
        )


def _get_or_create_user(
    users: UserService,
    login: str,
    password: str,
    role: UserRole,
    session: Session,
    password_hasher: PasswordHasher,
) -> User:
    user = users.find_user(login)
    if user is None:
        return users.create_user(login, password, role)

    user.password_hash = password_hasher.hash(password)
    user.role = role.value
    session.flush()
    return user
