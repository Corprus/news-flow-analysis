from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from accounting.exceptions import InsufficientBalanceError
from accounting.models import Account, Transaction, TransactionReason
from accounting.service import AccountingService
from users.models import Organization, User


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Organization.__table__.create(engine)
    User.__table__.create(engine)
    Account.__table__.create(engine)
    Transaction.__table__.create(engine)

    with Session(engine) as session:
        yield session


def _create_organization_with_user(
    session: Session,
    *,
    organization_name: str,
    login: str,
) -> tuple[UUID, UUID]:
    organization = Organization(name=organization_name)
    session.add(organization)
    session.flush()

    user = User(
        organization_id=organization.id,
        login=login,
        password_hash="test-password-hash",
        role="user",
    )
    session.add(user)
    session.flush()
    return UUID(organization.id), UUID(user.id)


def _transaction_count(session: Session, organization_id: UUID) -> int:
    statement = (
        select(func.count())
        .select_from(Transaction)
        .where(Transaction.organization_id == str(organization_id))
    )
    return session.scalar(statement) or 0


def _assert_ledger_matches_balance(session: Session, organization_id: UUID) -> None:
    balance = session.get(Account, str(organization_id))
    transaction_total = session.scalar(
        select(func.coalesce(func.sum(Transaction.amount), 0)).where(
            Transaction.organization_id == str(organization_id)
        )
    )

    assert balance is not None
    assert balance.balance == Decimal(transaction_total)


def test_failed_withdrawal_does_not_change_financial_state(session: Session) -> None:
    organization_id, user_id = _create_organization_with_user(
        session,
        organization_name="Acme",
        login="alice",
    )
    accounting = AccountingService(session)
    accounting.add_credit(organization_id, user_id, Decimal("10.00"))
    session.flush()

    balance_before = accounting.get_balance(organization_id)
    transactions_before = _transaction_count(session, organization_id)

    with pytest.raises(InsufficientBalanceError):
        accounting.withdraw_credit(
            user_id,
            Decimal("10.01"),
            TransactionReason.NEWS_SEARCH,
        )

    assert accounting.get_balance(organization_id) == balance_before
    assert _transaction_count(session, organization_id) == transactions_before
    _assert_ledger_matches_balance(session, organization_id)


def test_organization_balances_are_isolated(session: Session) -> None:
    first_organization_id, first_user_id = _create_organization_with_user(
        session,
        organization_name="First",
        login="first-user",
    )
    second_organization_id, second_user_id = _create_organization_with_user(
        session,
        organization_name="Second",
        login="second-user",
    )
    accounting = AccountingService(session)

    accounting.add_credit(first_organization_id, first_user_id, Decimal("25.00"))
    accounting.add_credit(second_organization_id, second_user_id, Decimal("7.00"))
    accounting.withdraw_credit(
        first_user_id,
        Decimal("3.00"),
        TransactionReason.NEWS_ADD,
    )

    assert accounting.get_balance(first_organization_id) == Decimal("22.00")
    assert accounting.get_balance(second_organization_id) == Decimal("7.00")
    assert _transaction_count(session, first_organization_id) == 2
    assert _transaction_count(session, second_organization_id) == 1
    _assert_ledger_matches_balance(session, first_organization_id)
    _assert_ledger_matches_balance(session, second_organization_id)


def test_transaction_records_actor_without_moving_balance_to_user(session: Session) -> None:
    organization_id, first_user_id = _create_organization_with_user(
        session,
        organization_name="Shared",
        login="first-member",
    )
    organization = session.get(Organization, str(organization_id))
    assert organization is not None
    second_user = User(
        organization_id=organization.id,
        login="second-member",
        password_hash="test-password-hash",
        role="user",
    )
    session.add(second_user)
    session.flush()
    second_user_id = UUID(second_user.id)
    accounting = AccountingService(session)

    accounting.add_credit(organization_id, first_user_id, Decimal("10.00"))
    accounting.withdraw_credit(
        second_user_id,
        Decimal("2.50"),
        TransactionReason.NEWS_SEARCH,
    )

    transactions = accounting.get_transaction_history(organization_id)
    withdrawal = next(transaction for transaction in transactions if transaction.amount < 0)

    assert withdrawal.organization_id == str(organization_id)
    assert withdrawal.actor_user_id == str(second_user_id)
    assert accounting.get_balance(organization_id) == Decimal("7.50")
    _assert_ledger_matches_balance(session, organization_id)


def test_batch_id_groups_related_withdrawals(session: Session) -> None:
    organization_id, user_id = _create_organization_with_user(
        session,
        organization_name="Batch",
        login="batch-user",
    )
    accounting = AccountingService(session)
    accounting.add_credit(organization_id, user_id, Decimal("10.00"))
    batch_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

    accounting.withdraw_credit(
        user_id,
        Decimal("1.00"),
        TransactionReason.NEWS_ADD,
        UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        batch_id,
    )
    accounting.withdraw_credit(
        user_id,
        Decimal("1.00"),
        TransactionReason.NEWS_ADD,
        UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        batch_id,
    )

    withdrawals = [
        transaction
        for transaction in accounting.get_transaction_history(organization_id)
        if transaction.amount < 0
    ]
    assert len(withdrawals) == 2
    assert {transaction.batch_id for transaction in withdrawals} == {str(batch_id)}


def test_admin_adjustment_can_add_and_withdraw_credit(session: Session) -> None:
    organization_id, admin_id = _create_organization_with_user(
        session,
        organization_name="Managed",
        login="admin",
    )
    accounting = AccountingService(session)

    accounting.adjust_credit(organization_id, admin_id, Decimal("15.00"))
    accounting.adjust_credit(organization_id, admin_id, Decimal("-4"))

    assert accounting.get_balance(organization_id) == Decimal("11.00")
    transactions = accounting.get_transaction_history(organization_id)
    assert {transaction.reason for transaction in transactions} == {
        TransactionReason.CREDIT_ADD.value,
        TransactionReason.CREDIT_WITHDRAW.value,
    }
    _assert_ledger_matches_balance(session, organization_id)


def test_admin_adjustment_rejects_overdraft(session: Session) -> None:
    organization_id, admin_id = _create_organization_with_user(
        session,
        organization_name="Managed",
        login="admin",
    )
    accounting = AccountingService(session)

    with pytest.raises(InsufficientBalanceError):
        accounting.adjust_credit(organization_id, admin_id, Decimal("-1"))

    assert accounting.get_balance(organization_id) == Decimal("0.00")
    assert _transaction_count(session, organization_id) == 0


def test_admin_adjustment_rejects_fractional_amount(session: Session) -> None:
    organization_id, admin_id = _create_organization_with_user(
        session,
        organization_name="Managed",
        login="admin",
    )
    accounting = AccountingService(session)

    with pytest.raises(ValueError, match="whole number"):
        accounting.adjust_credit(organization_id, admin_id, Decimal("1.5"))

    assert accounting.get_balance(organization_id) == Decimal("0.00")
    assert _transaction_count(session, organization_id) == 0
