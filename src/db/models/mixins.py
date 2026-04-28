from __future__ import annotations

from typing import TypeVar

from sqlalchemy.orm import Session

T = TypeVar("T")


class CrudMixin:
    @classmethod
    def get(cls: type[T], session: Session, entity_id: object) -> T | None:
        return session.get(cls, entity_id)

    def save(self, session: Session) -> None:
        session.add(self)
        session.flush()

    def delete(self, session: Session) -> None:
        session.delete(self)
        session.flush()
