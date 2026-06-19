from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from accounting.service import AccountingService
from news.importers import NewsImportError, news_importers
from news.models import ArticleStatus, ArticleVisibility, NewsArticle
from news.service import NewsService
from settings import Settings
from users.models import Organization, User, UserRole
from users.passwords import PasswordHasher
from users.service import UserService


@dataclass(frozen=True)
class DemoSeedResult:
    article_ids_to_process: list[str]
    imported_article_count: int


@dataclass(frozen=True)
class DemoUserSpec:
    login: str
    password: str
    role: UserRole


def validate_demo_settings(settings: Settings) -> None:
    if settings.demo_drop_db and not settings.demo_mode:
        raise RuntimeError("DEMO_DROP_DB requires DEMO_MODE=true")
    if settings.demo_mode and settings.app_env.lower() in {"prod", "production"}:
        raise RuntimeError("Demo mode is disabled in production")


def seed_demo(session: Session, settings: Settings) -> DemoSeedResult:
    password_hasher = PasswordHasher(settings.password_hash_secret)
    users = UserService(session, password_hasher)
    accounting = AccountingService(session)

    primary_organization = _get_or_create_organization(session, "Demo Research")
    partner_organization = _get_or_create_organization(session, "Partner Analytics")
    admin_organization = _get_or_create_organization(session, "News Flow Administration")

    demo_publisher = _get_or_update_user(
        users=users,
        session=session,
        password_hasher=password_hasher,
        organization=primary_organization,
        spec=DemoUserSpec(
            settings.demo_user_login,
            settings.demo_user_password,
            UserRole.PUBLISHER,
        ),
    )
    _get_or_update_user(
        users=users,
        session=session,
        password_hasher=password_hasher,
        organization=primary_organization,
        spec=DemoUserSpec("analyst", "analyst12345", UserRole.USER),
    )
    partner_publisher = _get_or_update_user(
        users=users,
        session=session,
        password_hasher=password_hasher,
        organization=partner_organization,
        spec=DemoUserSpec("partner_publisher", "partner12345", UserRole.PUBLISHER),
    )
    _get_or_update_user(
        users=users,
        session=session,
        password_hasher=password_hasher,
        organization=partner_organization,
        spec=DemoUserSpec("partner_user", "partner12345", UserRole.USER),
    )
    _get_or_update_user(
        users=users,
        session=session,
        password_hasher=password_hasher,
        organization=admin_organization,
        spec=DemoUserSpec(
            settings.demo_admin_login,
            settings.demo_admin_password,
            UserRole.ADMIN,
        ),
    )

    for user in (demo_publisher, partner_publisher):
        _ensure_credit(
            accounting,
            user,
            settings.demo_initial_credit,
        )

    articles = _load_demo_articles(settings.demo_news_path)
    news = NewsService(session)
    result = news.import_user_articles(
        user_id=UUID(demo_publisher.id),
        format_id="lenta",
        articles=articles,
    )
    article_ids_to_process = _publish_demo_articles(session, result.article_ids)
    session.flush()
    return DemoSeedResult(
        article_ids_to_process=article_ids_to_process,
        imported_article_count=len(result.article_ids),
    )


def _get_or_create_organization(session: Session, name: str) -> Organization:
    organization = session.execute(
        select(Organization).where(Organization.name == name)
    ).scalars().first()
    if organization is not None:
        return organization
    organization = Organization(name=name)
    session.add(organization)
    session.flush()
    return organization


def _get_or_update_user(
    *,
    users: UserService,
    session: Session,
    password_hasher: PasswordHasher,
    organization: Organization,
    spec: DemoUserSpec,
) -> User:
    user = users.find_user(spec.login)
    if user is None:
        return users.create_user(
            spec.login,
            spec.password,
            spec.role,
            organization_id=UUID(organization.id),
        )

    user.organization_id = organization.id
    user.password_hash = password_hasher.hash(spec.password)
    user.role = spec.role.value
    session.flush()
    return user


def _ensure_credit(
    accounting: AccountingService,
    user: User,
    target_balance: Decimal,
) -> None:
    organization_id = UUID(user.organization_id)
    current_balance = accounting.get_balance(organization_id)
    if current_balance < target_balance:
        accounting.add_credit(
            organization_id,
            UUID(user.id),
            target_balance - current_balance,
        )


def _load_demo_articles(path_value: str):
    path = Path(path_value)
    if not path.is_file():
        raise RuntimeError(
            f"Demo news fixture was not found: {path}. "
            "Run scripts/build_demo_fixture.py or set DEMO_NEWS_PATH."
        )
    try:
        return news_importers.parse("lenta", path.read_bytes())
    except NewsImportError as exc:
        raise RuntimeError(f"Invalid demo news fixture {path}: {exc}") from exc


def _publish_demo_articles(session: Session, article_ids: list[str]) -> list[str]:
    articles = list(
        session.execute(
            select(NewsArticle).where(NewsArticle.id.in_(article_ids))
        ).scalars()
    )
    to_process: list[str] = []
    for article in articles:
        if article.status == ArticleStatus.PROCESSED.value:
            continue
        article.visibility = ArticleVisibility.PUBLIC.value
        article.status = ArticleStatus.PENDING.value
        to_process.append(article.id)
    return to_process
