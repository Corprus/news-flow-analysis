from __future__ import annotations

from enum import StrEnum
from uuid import uuid4

from sqlalchemy import String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from db.database import Base
from db.models.mixins import CrudMixin


class UserRole(StrEnum):
    USER = "user"
    ADMIN = "admin"


class User(Base, CrudMixin):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    login: Mapped[str] = mapped_column(
        String(128),
        unique=True,
        index=True,
        nullable=False,
    )
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    role: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UserRole.USER.value,
    )

    def __repr__(self) -> str:
        return f"User(id={self.id}, login={self.login}, role={self.role})"
