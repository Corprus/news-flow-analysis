from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from accounting.exceptions import InsufficientBalanceError, UserAccountNotFoundError
from accounting.models import Account, Transaction, TransactionReason
from users.models import User


class AccountingService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_credit(
        self,
        user_id: UUID,
        amount: Decimal,
        reason: TransactionReason = TransactionReason.CREDIT_ADD,
        reference_id: UUID | None = None,
    ) -> UUID:
        if amount <= 0:
            raise ValueError("amount must be > 0")
        self._ensure_user_exists(user_id)

        account = self._get_or_create_account_locked(user_id)
        account.balance += amount
        transaction = self._create_transaction(user_id, amount, reason, reference_id)
        return UUID(transaction.id)

    def withdraw_credit(
        self,
        user_id: UUID,
        amount: Decimal,
        reason: TransactionReason,
        reference_id: UUID | None = None,
    ) -> UUID:
        if amount <= 0:
            raise ValueError("amount must be > 0")
        self._ensure_user_exists(user_id)

        account = self._get_or_create_account_locked(user_id)
        if account.balance < amount:
            raise InsufficientBalanceError()

        account.balance -= amount
        transaction = self._create_transaction(user_id, -amount, reason, reference_id)
        return UUID(transaction.id)

    def get_balance(self, user_id: UUID) -> Decimal:
        account = self._session.get(Account, str(user_id))
        return account.balance if account is not None else Decimal("0.00")

    def get_transaction_history(
        self,
        user_id: UUID,
        limit: int = 50,
        offset: int = 0,
        reason: TransactionReason | None = None,
    ) -> list[Transaction]:
        statement = (
            select(Transaction)
            .where(Transaction.user_id == str(user_id))
            .order_by(Transaction.timestamp.desc())
        )
        if reason is not None:
            statement = statement.where(Transaction.reason == reason.value)
        statement = statement.limit(limit).offset(offset)
        return list(self._session.execute(statement).scalars().all())

    def _get_or_create_account_locked(self, user_id: UUID) -> Account:
        statement = select(Account).where(Account.user_id == str(user_id)).with_for_update()
        account = self._session.execute(statement).scalars().first()
        if account is not None:
            return account

        account = Account(user_id=str(user_id), balance=Decimal("0.00"))
        self._session.add(account)
        self._session.flush()
        return account

    def _create_transaction(
        self,
        user_id: UUID,
        amount: Decimal,
        reason: TransactionReason,
        reference_id: UUID | None,
    ) -> Transaction:
        transaction = Transaction(
            user_id=str(user_id),
            amount=amount,
            reason=reason.value,
            reference_id=str(reference_id) if reference_id is not None else None,
        )
        self._session.add(transaction)
        self._session.flush()
        return transaction

    def _ensure_user_exists(self, user_id: UUID) -> None:
        if self._session.get(User, str(user_id)) is None:
            raise UserAccountNotFoundError()
