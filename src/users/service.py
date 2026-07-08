from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from accounting.models import Account
from users.exceptions import (
    InvalidCredentialsError,
    LastAdministratorError,
    UserAlreadyExistsError,
    UserNotFoundError,
)
from users.models import (
    MAX_ACCESS_EXPIRES_AT,
    AdminAuditLog,
    LicenseType,
    Organization,
    User,
    UserRole,
)
from users.passwords import PasswordHasher
from users.tokens import AccessTokenHandler


class UserService:
    """Сервис управления пользователями и их принадлежностью к организациям."""

    def __init__(self, session: Session, password_hasher: PasswordHasher) -> None:
        """Создать сервис с БД-сессией и хешером паролей."""
        self._session = session
        self._password_hasher = password_hasher

    def create_user(
        self,
        login: str,
        password: str,
        role: UserRole = UserRole.USER,
        organization_id: UUID | None = None,
    ) -> User:
        """Создать пользователя, при необходимости вместе с новой организацией."""
        if self.find_user(login) is not None:
            raise UserAlreadyExistsError()

        if organization_id is None:
            organization = Organization(name=login)
            self._session.add(organization)
            self._session.flush()
            organization_id = UUID(organization.id)
        elif self._session.get(Organization, str(organization_id)) is None:
            raise ValueError("Organization does not exist")

        user = User(
            organization_id=str(organization_id),
            login=login,
            password_hash=self._password_hasher.hash(password),
            role=role.value,
        )
        self._session.add(user)
        self._session.flush()
        return user

    def delete_user(self, user_id: UUID) -> None:
        """Удалить пользователя, не позволяя удалить последнего администратора."""
        user = self.find_user_by_id(user_id)
        if user is None:
            raise UserNotFoundError()
        if user.role == UserRole.ADMIN.value:
            self._ensure_another_administrator_exists(user_id)
        self._session.delete(user)
        self._session.flush()

    def update_role(self, user_id: UUID, role: UserRole) -> User:
        """Изменить роль пользователя с защитой последнего администратора."""
        user = self.find_user_by_id(user_id)
        if user is None:
            raise UserNotFoundError()
        if user.role == UserRole.ADMIN.value and role != UserRole.ADMIN:
            self._ensure_another_administrator_exists(user_id)
        user.role = role.value
        self._session.flush()
        return user

    def update_user(
        self,
        user_id: UUID,
        *,
        login: str,
        role: UserRole,
        organization_id: UUID,
    ) -> User:
        """Обновить логин, роль и организацию пользователя."""
        user = self.find_user_by_id(user_id)
        if user is None:
            raise UserNotFoundError()
        existing = self.find_user(login)
        if existing is not None and existing.id != user.id:
            raise UserAlreadyExistsError()
        if self._session.get(Organization, str(organization_id)) is None:
            raise ValueError("Organization does not exist")
        if user.role == UserRole.ADMIN.value and role != UserRole.ADMIN:
            self._ensure_another_administrator_exists(user_id)
        user.login = login
        user.role = role.value
        user.organization_id = str(organization_id)
        self._session.flush()
        return user

    def find_user(self, login: str) -> User | None:
        """Найти пользователя по логину."""
        query = select(User).where(User.login == login)
        return self._session.execute(query).scalars().first()

    def find_user_by_id(self, user_id: UUID) -> User | None:
        """Найти пользователя по UUID."""
        return self._session.get(User, str(user_id))

    def list_users(self, role: UserRole | None = None) -> Sequence[User]:
        """Вернуть пользователей, опционально отфильтрованных по роли."""
        query = select(User).order_by(User.login)
        if role is not None:
            query = query.where(User.role == role.value)
        return self._session.execute(query).scalars().all()

    def _ensure_another_administrator_exists(self, user_id: UUID) -> None:
        administrators = list(
            self._session.execute(
                select(User)
                .where(User.role == UserRole.ADMIN.value)
                .with_for_update()
            ).scalars()
        )
        if not any(user.id != str(user_id) for user in administrators):
            raise LastAdministratorError()


class OrganizationService:
    """Сервис административного управления организациями."""

    def __init__(self, session: Session) -> None:
        """Создать сервис поверх текущей SQLAlchemy-сессии."""
        self._session = session

    def create(
        self,
        name: str,
        *,
        license_type: LicenseType = LicenseType.SUBSCRIPTION,
        access_expires_at: datetime = MAX_ACCESS_EXPIRES_AT,
    ) -> Organization:
        """Создать организацию с параметрами лицензии."""
        normalized_name = name.strip()
        if self.find_by_name(normalized_name) is not None:
            raise ValueError("Organization already exists")
        organization = Organization(
            name=normalized_name,
            license_type=license_type.value,
            access_expires_at=access_expires_at,
        )
        self._session.add(organization)
        self._session.flush()
        return organization

    def update(
        self,
        organization_id: UUID,
        *,
        name: str,
        license_type: LicenseType,
        access_expires_at: datetime,
    ) -> Organization:
        """Обновить название и параметры лицензии организации."""
        organization = self.find_by_id(organization_id)
        if organization is None:
            raise ValueError("Organization does not exist")
        normalized_name = name.strip()
        existing = self.find_by_name(normalized_name)
        if existing is not None and existing.id != organization.id:
            raise ValueError("Organization already exists")
        organization.name = normalized_name
        organization.license_type = license_type.value
        organization.access_expires_at = access_expires_at
        self._session.flush()
        return organization

    def update_name(self, organization_id: UUID, name: str) -> Organization:
        """Переименовать организацию без изменения лицензии."""
        organization = self.find_by_id(organization_id)
        if organization is None:
            raise ValueError("Organization does not exist")
        return self.update(
            organization_id,
            name=name,
            license_type=LicenseType(organization.license_type),
            access_expires_at=organization.access_expires_at,
        )

    def find_by_id(self, organization_id: UUID) -> Organization | None:
        """Найти организацию по UUID."""
        return self._session.get(Organization, str(organization_id))

    def find_by_name(self, name: str) -> Organization | None:
        """Найти организацию по точному названию."""
        return self._session.execute(
            select(Organization).where(Organization.name == name)
        ).scalars().first()

    def list_with_summary(self) -> list[tuple[Organization, int, Decimal]]:
        """Вернуть организации вместе с числом пользователей и балансом."""
        statement = (
            select(
                Organization,
                func.count(User.id).label("user_count"),
                func.coalesce(Account.balance, 0).label("balance"),
            )
            .outerjoin(User, User.organization_id == Organization.id)
            .outerjoin(Account, Account.organization_id == Organization.id)
            .group_by(Organization.id, Account.balance)
            .order_by(Organization.name)
        )
        return list(self._session.execute(statement).all())


class AdminAuditService:
    """Сервис записи и чтения административного аудита."""

    def __init__(self, session: Session) -> None:
        """Создать сервис поверх текущей SQLAlchemy-сессии."""
        self._session = session

    def record(
        self,
        *,
        actor_user_id: UUID | None,
        action: str,
        target_type: str,
        target_id: UUID | str | None,
        details: dict | None = None,
    ) -> AdminAuditLog:
        """Записать административное действие пользователя."""
        entry = AdminAuditLog(
            actor_user_id=str(actor_user_id) if actor_user_id is not None else None,
            action=action,
            target_type=target_type,
            target_id=str(target_id) if target_id is not None else None,
            details=details or {},
        )
        self._session.add(entry)
        self._session.flush()
        return entry

    def list_entries(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        action: str | None = None,
    ) -> Sequence[AdminAuditLog]:
        """Вернуть последние записи аудита, при необходимости по типу действия."""
        statement = select(AdminAuditLog).order_by(
            AdminAuditLog.created_at.desc(),
            AdminAuditLog.id.desc(),
        )
        if action:
            statement = statement.where(AdminAuditLog.action == action)
        return self._session.execute(
            statement.limit(limit).offset(offset)
        ).scalars().all()


class AuthService:
    """Сервис входа пользователя и выпуска access token."""

    def __init__(
        self,
        user_service: UserService,
        password_hasher: PasswordHasher,
        token_handler: AccessTokenHandler,
    ) -> None:
        """Связать user-service, проверку пароля и выпуск токенов."""
        self._user_service = user_service
        self._password_hasher = password_hasher
        self._token_handler = token_handler

    def login(self, login: str, password: str) -> str:
        """Проверить пару логин/пароль и вернуть access token."""
        user = self._user_service.find_user(login)
        if user is None:
            raise InvalidCredentialsError()
        if not self._password_hasher.verify(password, user.password_hash):
            raise InvalidCredentialsError()
        return self._token_handler.create_access_token(
            UUID(user.id),
            UUID(user.organization_id),
            user.role,
        )
