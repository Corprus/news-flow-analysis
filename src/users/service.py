from __future__ import annotations

from collections.abc import Sequence
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
from users.models import AdminAuditLog, Organization, User, UserRole
from users.passwords import PasswordHasher
from users.tokens import AccessTokenHandler


class UserService:
    def __init__(self, session: Session, password_hasher: PasswordHasher) -> None:
        self._session = session
        self._password_hasher = password_hasher

    def create_user(
        self,
        login: str,
        password: str,
        role: UserRole = UserRole.USER,
        organization_id: UUID | None = None,
    ) -> User:
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
        user = self.find_user_by_id(user_id)
        if user is None:
            raise UserNotFoundError()
        if user.role == UserRole.ADMIN.value:
            self._ensure_another_administrator_exists(user_id)
        self._session.delete(user)
        self._session.flush()

    def update_role(self, user_id: UUID, role: UserRole) -> User:
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
        query = select(User).where(User.login == login)
        return self._session.execute(query).scalars().first()

    def find_user_by_id(self, user_id: UUID) -> User | None:
        return self._session.get(User, str(user_id))

    def list_users(self, role: UserRole | None = None) -> Sequence[User]:
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
    def __init__(self, session: Session) -> None:
        self._session = session

    def create(self, name: str) -> Organization:
        normalized_name = name.strip()
        if self.find_by_name(normalized_name) is not None:
            raise ValueError("Organization already exists")
        organization = Organization(name=normalized_name)
        self._session.add(organization)
        self._session.flush()
        return organization

    def update_name(self, organization_id: UUID, name: str) -> Organization:
        organization = self.find_by_id(organization_id)
        if organization is None:
            raise ValueError("Organization does not exist")
        normalized_name = name.strip()
        existing = self.find_by_name(normalized_name)
        if existing is not None and existing.id != organization.id:
            raise ValueError("Organization already exists")
        organization.name = normalized_name
        self._session.flush()
        return organization

    def find_by_id(self, organization_id: UUID) -> Organization | None:
        return self._session.get(Organization, str(organization_id))

    def find_by_name(self, name: str) -> Organization | None:
        return self._session.execute(
            select(Organization).where(Organization.name == name)
        ).scalars().first()

    def list_with_summary(self) -> list[tuple[Organization, int, Decimal]]:
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
    def __init__(self, session: Session) -> None:
        self._session = session

    def record(
        self,
        *,
        actor_user_id: UUID,
        action: str,
        target_type: str,
        target_id: UUID | str | None,
        details: dict | None = None,
    ) -> AdminAuditLog:
        entry = AdminAuditLog(
            actor_user_id=str(actor_user_id),
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
    def __init__(
        self,
        user_service: UserService,
        password_hasher: PasswordHasher,
        token_handler: AccessTokenHandler,
    ) -> None:
        self._user_service = user_service
        self._password_hasher = password_hasher
        self._token_handler = token_handler

    def login(self, login: str, password: str) -> str:
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
