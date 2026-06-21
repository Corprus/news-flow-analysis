from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from accounting.exceptions import InsufficientBalanceError, UserAccountNotFoundError
from accounting.models import Account, Transaction, TransactionReason
from users.models import Organization, User


class AccountingService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_credit(
        self,
        organization_id: UUID,
        actor_user_id: UUID,
        amount: Decimal,
        reason: TransactionReason = TransactionReason.CREDIT_ADD,
        reference_id: UUID | None = None,
    ) -> UUID:
        if amount <= 0:
            raise ValueError("amount must be > 0")
        self._ensure_organization_exists(organization_id)
        self._ensure_user_exists(actor_user_id)

        account = self._get_or_create_account_locked(organization_id)
        account.balance += amount
        transaction = self._create_transaction(
            organization_id,
            actor_user_id,
            amount,
            reason,
            reference_id,
        )
        return UUID(transaction.id)

    def adjust_credit(
        self,
        organization_id: UUID,
        actor_user_id: UUID,
        amount: Decimal,
    ) -> UUID:
        if amount == 0:
            raise ValueError("amount must not be zero")
        if amount != amount.to_integral_value():
            raise ValueError("amount must be a whole number")
        if amount > 0:
            return self.add_credit(organization_id, actor_user_id, amount)

        self._ensure_organization_exists(organization_id)
        self._ensure_user_exists(actor_user_id)
        account = self._get_or_create_account_locked(organization_id)
        if account.balance < -amount:
            raise InsufficientBalanceError()
        account.balance += amount
        transaction = self._create_transaction(
            organization_id,
            actor_user_id,
            amount,
            TransactionReason.CREDIT_WITHDRAW,
            None,
        )
        return UUID(transaction.id)

    def withdraw_credit(
        self,
        user_id: UUID,
        amount: Decimal,
        reason: TransactionReason,
        reference_id: UUID | None = None,
        batch_id: UUID | None = None,
    ) -> UUID:
        if amount <= 0:
            raise ValueError("amount must be > 0")
        user = self._get_user(user_id)
        organization_id = UUID(user.organization_id)

        account = self._get_or_create_account_locked(organization_id)
        if account.balance < amount:
            raise InsufficientBalanceError()

        account.balance -= amount
        transaction = self._create_transaction(
            organization_id,
            user_id,
            -amount,
            reason,
            reference_id,
            batch_id,
        )
        return UUID(transaction.id)

    def get_balance(self, organization_id: UUID) -> Decimal:
        account = self._session.get(Account, str(organization_id))
        return account.balance if account is not None else Decimal("0.00")

    def get_transaction_history(
        self,
        organization_id: UUID,
        limit: int | None = 50,
        offset: int = 0,
        reason: TransactionReason | None = None,
    ) -> list[Transaction]:
        statement = (
            select(Transaction)
            .where(Transaction.organization_id == str(organization_id))
            .order_by(Transaction.timestamp.desc())
        )
        if reason is not None:
            statement = statement.where(Transaction.reason == reason.value)
        if limit is not None:
            statement = statement.limit(limit).offset(offset)
        return list(self._session.execute(statement).scalars().all())

    def _get_or_create_account_locked(self, organization_id: UUID) -> Account:
        statement = (
            select(Account)
            .where(Account.organization_id == str(organization_id))
            .with_for_update()
        )
        account = self._session.execute(statement).scalars().first()
        if account is not None:
            return account

        account = Account(organization_id=str(organization_id), balance=Decimal("0.00"))
        self._session.add(account)
        self._session.flush()
        return account

    def _create_transaction(
        self,
        organization_id: UUID,
        actor_user_id: UUID,
        amount: Decimal,
        reason: TransactionReason,
        reference_id: UUID | None,
        batch_id: UUID | None = None,
    ) -> Transaction:
        transaction = Transaction(
            organization_id=str(organization_id),
            actor_user_id=str(actor_user_id),
            amount=amount,
            reason=reason.value,
            reference_id=str(reference_id) if reference_id is not None else None,
            batch_id=str(batch_id) if batch_id is not None else None,
        )
        self._session.add(transaction)
        self._session.flush()
        return transaction

    def _ensure_user_exists(self, user_id: UUID) -> None:
        self._get_user(user_id)

    def _get_user(self, user_id: UUID) -> User:
        user = self._session.get(User, str(user_id))
        if user is None:
            raise UserAccountNotFoundError()
        return user

    def _ensure_organization_exists(self, organization_id: UUID) -> None:
        if self._session.get(Organization, str(organization_id)) is None:
            raise UserAccountNotFoundError()
