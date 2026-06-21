from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field

from users.deps import (
    CurrentUser,
    authenticate,
    ensure_admin,
    get_admin_audit_service,
    get_auth_service,
    get_organization_service,
    get_user_service,
)
from users.exceptions import (
    InvalidCredentialsError,
    LastAdministratorError,
    UserAlreadyExistsError,
    UserNotFoundError,
)
from users.models import AdminAuditLog, Organization, User, UserRole
from users.service import AdminAuditService, AuthService, OrganizationService, UserService

router = APIRouter(prefix="/v1/users", tags=["users"])
auth_router = APIRouter(prefix="/v1/auth", tags=["auth"])
organization_router = APIRouter(prefix="/v1/organizations", tags=["organizations"])
admin_router = APIRouter(prefix="/v1/admin", tags=["admin"])

AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]
UserServiceDep = Annotated[UserService, Depends(get_user_service)]
OrganizationServiceDep = Annotated[OrganizationService, Depends(get_organization_service)]
AdminAuditServiceDep = Annotated[AdminAuditService, Depends(get_admin_audit_service)]
CurrentUserDep = Annotated[CurrentUser, Depends(authenticate)]
RoleQuery = Annotated[UserRole | None, Query()]


class CreateUserRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    login: str = Field(min_length=3, max_length=128)
    password: str = Field(min_length=8, max_length=256)


class AdminCreateUserRequest(CreateUserRequest):
    organization_id: UUID
    role: UserRole = UserRole.USER


class UpdateUserRoleRequest(BaseModel):
    role: UserRole


class UpdateUserRequest(BaseModel):
    login: str = Field(min_length=3, max_length=128)
    role: UserRole
    organization_id: UUID


class CreateOrganizationRequest(BaseModel):
    name: str = Field(min_length=2, max_length=256)


class UpdateOrganizationRequest(BaseModel):
    name: str = Field(min_length=2, max_length=256)


class LoginRequest(BaseModel):
    login: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: UUID
    organization_id: UUID
    login: str
    role: UserRole


class CurrentUserResponse(UserResponse):
    organization_name: str


class OrganizationResponse(BaseModel):
    id: UUID
    name: str
    created_at: datetime
    user_count: int
    balance: str


class AdminAuditResponse(BaseModel):
    id: UUID
    actor_user_id: UUID | None
    action: str
    target_type: str
    target_id: str | None
    details: dict
    created_at: datetime


def _to_response(user: User) -> UserResponse:
    return UserResponse(
        id=UUID(user.id),
        organization_id=UUID(user.organization_id),
        login=user.login,
        role=UserRole(user.role),
    )


@auth_router.post("/login", response_model=TokenResponse)
def login(request: LoginRequest, auth: AuthServiceDep) -> TokenResponse:
    try:
        return TokenResponse(access_token=auth.login(request.login, request.password))
    except InvalidCredentialsError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid login or password",
        ) from exc


@router.post("", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def create_user(request: CreateUserRequest, users: UserServiceDep) -> UserResponse:
    try:
        return _to_response(users.create_user(request.login, request.password))
    except UserAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists",
        ) from exc


@router.get("", response_model=list[UserResponse])
def list_users(
    users: UserServiceDep,
    current_user: CurrentUserDep,
    role: RoleQuery = None,
) -> list[UserResponse]:
    ensure_admin(current_user)
    return [_to_response(user) for user in users.list_users(role=role)]


@router.get("/me", response_model=CurrentUserResponse)
def get_me(
    current_user: CurrentUserDep,
    users: UserServiceDep,
    organizations: OrganizationServiceDep,
) -> CurrentUserResponse:
    user = users.find_user_by_id(current_user.id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    organization = organizations.find_by_id(current_user.organization_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )
    return CurrentUserResponse(
        **_to_response(user).model_dump(),
        organization_name=organization.name,
    )


@router.get("/{user_id}", response_model=UserResponse)
def get_user(
    user_id: UUID,
    users: UserServiceDep,
    current_user: CurrentUserDep,
) -> UserResponse:
    ensure_admin(current_user)
    user = users.find_user_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return _to_response(user)


@router.patch("/{user_id}/role", response_model=UserResponse)
def update_user_role(
    user_id: UUID,
    request: UpdateUserRoleRequest,
    users: UserServiceDep,
    audit: AdminAuditServiceDep,
    current_user: CurrentUserDep,
) -> UserResponse:
    ensure_admin(current_user)
    if user_id == current_user.id and request.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An administrator cannot remove their own admin role",
        )
    user = users.find_user_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    previous_role = user.role
    try:
        updated = users.update_role(user_id, request.role)
    except LastAdministratorError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The last administrator cannot be demoted",
        ) from exc
    audit.record(
        actor_user_id=current_user.id,
        action="user.role.update",
        target_type="user",
        target_id=user_id,
        details={
            "login": user.login,
            "previous_role": previous_role,
            "role": updated.role,
        },
    )
    return _to_response(updated)


@router.patch("/{user_id}", response_model=UserResponse)
def update_user(
    user_id: UUID,
    request: UpdateUserRequest,
    users: UserServiceDep,
    organizations: OrganizationServiceDep,
    audit: AdminAuditServiceDep,
    current_user: CurrentUserDep,
) -> UserResponse:
    ensure_admin(current_user)
    if user_id == current_user.id and request.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An administrator cannot remove their own admin role",
        )
    user = users.find_user_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if organizations.find_by_id(request.organization_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )
    previous = {
        "login": user.login,
        "role": user.role,
        "organization_id": user.organization_id,
    }
    try:
        updated = users.update_user(
            user_id,
            login=request.login.strip(),
            role=request.role,
            organization_id=request.organization_id,
        )
    except UserAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists",
        ) from exc
    except LastAdministratorError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The last administrator cannot be demoted",
        ) from exc
    audit.record(
        actor_user_id=current_user.id,
        action="user.update",
        target_type="user",
        target_id=user_id,
        details={
            "previous": previous,
            "login": updated.login,
            "role": updated.role,
            "organization_id": updated.organization_id,
        },
    )
    return _to_response(updated)


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
def delete_user(
    user_id: UUID,
    users: UserServiceDep,
    audit: AdminAuditServiceDep,
    current_user: CurrentUserDep,
) -> None:
    ensure_admin(current_user)
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An administrator cannot delete their own account",
        )
    user = users.find_user_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    snapshot = {
        "login": user.login,
        "role": user.role,
        "organization_id": user.organization_id,
    }
    try:
        users.delete_user(user_id)
    except UserNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        ) from exc
    except LastAdministratorError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The last administrator cannot be deleted",
        ) from exc
    audit.record(
        actor_user_id=current_user.id,
        action="user.delete",
        target_type="user",
        target_id=user_id,
        details=snapshot,
    )


@admin_router.post(
    "/users",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
)
def admin_create_user(
    request: AdminCreateUserRequest,
    users: UserServiceDep,
    organizations: OrganizationServiceDep,
    audit: AdminAuditServiceDep,
    current_user: CurrentUserDep,
) -> UserResponse:
    ensure_admin(current_user)
    if organizations.find_by_id(request.organization_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )
    try:
        user = users.create_user(
            request.login,
            request.password,
            request.role,
            organization_id=request.organization_id,
        )
    except UserAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists",
        ) from exc
    audit.record(
        actor_user_id=current_user.id,
        action="user.create",
        target_type="user",
        target_id=user.id,
        details={
            "login": user.login,
            "role": user.role,
            "organization_id": user.organization_id,
        },
    )
    return _to_response(user)


@organization_router.get("", response_model=list[OrganizationResponse])
def list_organizations(
    organizations: OrganizationServiceDep,
    current_user: CurrentUserDep,
) -> list[OrganizationResponse]:
    ensure_admin(current_user)
    return [
        OrganizationResponse(
            id=UUID(organization.id),
            name=organization.name,
            created_at=organization.created_at,
            user_count=user_count,
            balance=str(Decimal(balance)),
        )
        for organization, user_count, balance in organizations.list_with_summary()
    ]


@organization_router.post(
    "",
    response_model=OrganizationResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_organization(
    request: CreateOrganizationRequest,
    organizations: OrganizationServiceDep,
    audit: AdminAuditServiceDep,
    current_user: CurrentUserDep,
) -> OrganizationResponse:
    ensure_admin(current_user)
    try:
        organization = organizations.create(request.name)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    audit.record(
        actor_user_id=current_user.id,
        action="organization.create",
        target_type="organization",
        target_id=organization.id,
        details={"name": organization.name},
    )
    return _organization_response(organization)


@organization_router.patch("/{organization_id}", response_model=OrganizationResponse)
def update_organization(
    organization_id: UUID,
    request: UpdateOrganizationRequest,
    organizations: OrganizationServiceDep,
    audit: AdminAuditServiceDep,
    current_user: CurrentUserDep,
) -> OrganizationResponse:
    ensure_admin(current_user)
    organization = organizations.find_by_id(organization_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )
    previous_name = organization.name
    try:
        updated = organizations.update_name(organization_id, request.name)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    audit.record(
        actor_user_id=current_user.id,
        action="organization.update",
        target_type="organization",
        target_id=organization_id,
        details={"previous_name": previous_name, "name": updated.name},
    )
    return _organization_response(updated)


@admin_router.get("/audit", response_model=list[AdminAuditResponse])
def list_admin_audit(
    audit: AdminAuditServiceDep,
    current_user: CurrentUserDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    action: str | None = None,
) -> list[AdminAuditResponse]:
    ensure_admin(current_user)
    entries = audit.list_entries(limit=limit, offset=offset, action=action)
    return [_audit_response(entry) for entry in entries]


def _organization_response(organization: Organization) -> OrganizationResponse:
    return OrganizationResponse(
        id=UUID(organization.id),
        name=organization.name,
        created_at=organization.created_at,
        user_count=0,
        balance="0.00",
    )


def _audit_response(entry: AdminAuditLog) -> AdminAuditResponse:
    return AdminAuditResponse(
        id=UUID(entry.id),
        actor_user_id=UUID(entry.actor_user_id) if entry.actor_user_id else None,
        action=entry.action,
        target_type=entry.target_type,
        target_id=entry.target_id,
        details=entry.details,
        created_at=entry.created_at,
    )
