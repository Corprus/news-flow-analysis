from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from users.deps import CurrentUser, authenticate, ensure_admin, get_auth_service, get_user_service
from users.exceptions import InvalidCredentialsError, UserAlreadyExistsError, UserNotFoundError
from users.models import User, UserRole
from users.service import AuthService, UserService

router = APIRouter(prefix="/v1/users", tags=["users"])
auth_router = APIRouter(prefix="/v1/auth", tags=["auth"])
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]
UserServiceDep = Annotated[UserService, Depends(get_user_service)]
CurrentUserDep = Annotated[CurrentUser, Depends(authenticate)]
RoleQuery = Annotated[UserRole | None, Query()]


class CreateUserRequest(BaseModel):
    login: str = Field(min_length=3, max_length=128)
    password: str = Field(min_length=8, max_length=256)
    role: UserRole = UserRole.USER


class LoginRequest(BaseModel):
    login: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: UUID
    login: str
    role: UserRole


def _to_response(user: User) -> UserResponse:
    return UserResponse(id=UUID(user.id), login=user.login, role=UserRole(user.role))


@auth_router.post("/login", response_model=TokenResponse)
def login(
    request: LoginRequest,
    auth: AuthServiceDep,
) -> TokenResponse:
    try:
        return TokenResponse(access_token=auth.login(request.login, request.password))
    except InvalidCredentialsError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid login or password",
        ) from exc


@router.post("", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def create_user(
    request: CreateUserRequest,
    users: UserServiceDep,
) -> UserResponse:
    try:
        return _to_response(users.create_user(request.login, request.password, request.role))
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


@router.get("/me", response_model=UserResponse)
def get_me(
    current_user: CurrentUserDep,
    users: UserServiceDep,
) -> UserResponse:
    user = users.find_user_by_id(current_user.id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return _to_response(user)


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


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
def delete_user(
    user_id: UUID,
    users: UserServiceDep,
    current_user: CurrentUserDep,
) -> None:
    ensure_admin(current_user)
    try:
        users.delete_user(user_id)
    except UserNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found") from exc
