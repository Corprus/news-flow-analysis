from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from db.database import get_session
from settings import Settings, get_settings
from users.exceptions import InvalidAccessTokenError
from users.models import User, UserRole
from users.passwords import PasswordHasher
from users.service import AdminAuditService, AuthService, OrganizationService, UserService
from users.tokens import AccessTokenHandler


@dataclass(frozen=True)
class CurrentUser:
    id: UUID
    organization_id: UUID
    role: UserRole


def get_db_session() -> Iterator[Session]:
    with get_session() as session:
        yield session


SettingsDep = Annotated[Settings, Depends(get_settings)]
SessionDep = Annotated[Session, Depends(get_db_session)]
AuthorizationHeader = Annotated[str | None, Header()]


def get_password_hasher(settings: SettingsDep) -> PasswordHasher:
    return PasswordHasher(settings.password_hash_secret)


def get_token_handler(settings: SettingsDep) -> AccessTokenHandler:
    return AccessTokenHandler(
        secret=settings.access_token_secret,
        ttl_minutes=settings.access_token_ttl_minutes,
    )


def get_user_service(
    session: SessionDep,
    password_hasher: Annotated[PasswordHasher, Depends(get_password_hasher)],
) -> UserService:
    return UserService(session, password_hasher)


def get_organization_service(session: SessionDep) -> OrganizationService:
    return OrganizationService(session)


def get_admin_audit_service(session: SessionDep) -> AdminAuditService:
    return AdminAuditService(session)


def get_auth_service(
    users: Annotated[UserService, Depends(get_user_service)],
    password_hasher: Annotated[PasswordHasher, Depends(get_password_hasher)],
    token_handler: Annotated[AccessTokenHandler, Depends(get_token_handler)],
) -> AuthService:
    return AuthService(users, password_hasher, token_handler)


def authenticate(
    token_handler: Annotated[AccessTokenHandler, Depends(get_token_handler)],
    session: SessionDep,
    authorization: AuthorizationHeader = None,
) -> CurrentUser:
    if authorization is None or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization bearer token is required",
        )

    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = token_handler.verify_access_token(token)
    except InvalidAccessTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
        ) from exc

    user = session.get(User, payload["sub"])
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is no longer available",
        )
    return CurrentUser(
        id=UUID(user.id),
        organization_id=UUID(user.organization_id),
        role=UserRole(user.role),
    )


def ensure_admin(current_user: CurrentUser) -> None:
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role is required",
        )


def ensure_publisher(current_user: CurrentUser) -> None:
    if current_user.role not in {UserRole.PUBLISHER, UserRole.ADMIN}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Publisher role is required",
        )
