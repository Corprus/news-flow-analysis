from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from accounting.models import Account, Transaction
from accounting.routes import _to_response_group, get_admin_transactions
from accounting.service import AccountingService
from users.deps import CurrentUser
from users.models import Organization, User, UserRole


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Organization.__table__.create(engine)
    User.__table__.create(engine)
    Account.__table__.create(engine)
    Transaction.__table__.create(engine)

    with Session(engine) as session:
        yield session


def _create_user(
    session: Session,
    *,
    organization_name: str,
    login: str,
    role: UserRole,
) -> tuple[Organization, User]:
    organization = Organization(name=organization_name)
    session.add(organization)
    session.flush()
    user = User(
        organization_id=organization.id,
        login=login,
        password_hash="test-password-hash",
        role=role.value,
    )
    session.add(user)
    session.flush()
    return organization, user


def test_admin_transactions_include_operations_from_other_organizations(
    session: Session,
) -> None:
    admin_organization, admin = _create_user(
        session,
        organization_name="Administration",
        login="admin",
        role=UserRole.ADMIN,
    )
    customer_organization, customer = _create_user(
        session,
        organization_name="Customer",
        login="customer",
        role=UserRole.USER,
    )
    accounting = AccountingService(session)
    accounting.add_credit(
        UUID(customer_organization.id),
        UUID(customer.id),
        Decimal("10"),
    )

    transactions = get_admin_transactions(
        current_user=CurrentUser(
            id=UUID(admin.id),
            organization_id=UUID(admin_organization.id),
            role=UserRole.ADMIN,
        ),
        session=session,
        accounting=accounting,
        limit=100,
        offset=0,
        reason=None,
    )

    assert len(transactions) == 1
    assert transactions[0].organization_id == UUID(customer_organization.id)
    assert transactions[0].organization_name == "Customer"
    assert transactions[0].actor_user_id == UUID(customer.id)
    assert transactions[0].actor_login == "customer"


def test_non_admin_cannot_list_all_transactions(session: Session) -> None:
    organization, user = _create_user(
        session,
        organization_name="Customer",
        login="customer",
        role=UserRole.USER,
    )

    with pytest.raises(HTTPException) as error:
        get_admin_transactions(
            current_user=CurrentUser(
                id=UUID(user.id),
                organization_id=UUID(organization.id),
                role=UserRole.USER,
            ),
            session=session,
            accounting=AccountingService(session),
            limit=100,
            offset=0,
            reason=None,
        )

    assert error.value.status_code == 403


def test_batch_operation_response_preserves_aggregated_item_count() -> None:
    """Ответ операций сохраняет размер агрегированной пакетной публикации."""
    transaction = SimpleNamespace(
        id="11111111-1111-1111-1111-111111111111",
        organization_id="22222222-2222-2222-2222-222222222222",
        actor_user_id="33333333-3333-3333-3333-333333333333",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        amount=Decimal("-22.00"),
        reason="news_add",
        reference_id="44444444-4444-4444-4444-444444444444",
        batch_id="55555555-5555-5555-5555-555555555555",
        item_count=22,
    )

    response = _to_response_group(
        [transaction],
        article_by_id={},
        actor_login_by_id={},
        organization_name_by_id={},
    )

    assert response.batch_id == UUID(transaction.batch_id)
    assert response.item_count == 22
    assert response.reference_title is None
