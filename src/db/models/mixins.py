from __future__ import annotations

from typing import TypeVar

from sqlalchemy.orm import Session

T = TypeVar("T")


class CrudMixin:
    """Минимальные CRUD-helper методы для SQLAlchemy-моделей."""

    @classmethod
    def get(cls: type[T], session: Session, entity_id: object) -> T | None:
        """Получить сущность по primary key."""
        return session.get(cls, entity_id)

    def save(self, session: Session) -> None:
        """Добавить сущность в сессию и сразу выполнить flush."""
        session.add(self)
        session.flush()

    def delete(self, session: Session) -> None:
        """Удалить сущность из сессии и сразу выполнить flush."""
        session.delete(self)
        session.flush()
