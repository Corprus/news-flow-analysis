from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from users.exceptions import (
    InvalidCredentialsError,
    UserAlreadyExistsError,
    UserNotFoundError,
)
from users.models import User, UserRole
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
    ) -> User:
        if self.find_user(login) is not None:
            raise UserAlreadyExistsError()

        user = User(
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
        self._session.delete(user)
        self._session.flush()

    def find_user(self, login: str) -> User | None:
        query = select(User).where(User.login == login)
        return self._session.execute(query).scalars().first()

    def find_user_by_id(self, user_id: UUID) -> User | None:
        return self._session.get(User, str(user_id))

    def list_users(self, role: UserRole | None = None) -> Sequence[User]:
        query = select(User)
        if role is not None:
            query = query.where(User.role == role.value)
        return self._session.execute(query).scalars().all()


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
        return self._token_handler.create_access_token(UUID(user.id), user.role)
