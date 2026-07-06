from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from accounting.exceptions import InsufficientBalanceError, UserAccountNotFoundError
from accounting.models import Account, Transaction, TransactionReason
from users.models import LicenseType, Organization, User


class AccountingService:
    """Сервис баланса организации и финансовых операций по публикациям/поиску."""

    def __init__(self, session: Session) -> None:
        """Создать сервис поверх текущей SQLAlchemy-сессии."""
        self._session = session

    def add_credit(
        self,
        organization_id: UUID,
        actor_user_id: UUID,
        amount: Decimal,
        reason: TransactionReason = TransactionReason.CREDIT_ADD,
        reference_id: UUID | None = None,
    ) -> UUID:
        """Пополнить баланс организации и записать положительную транзакцию."""
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
        """Вручную скорректировать баланс организации целым числом кредитов."""
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
        """Списать кредиты с организации пользователя для платной операции."""
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

    def refund_credit(
        self,
        user_id: UUID,
        amount: Decimal,
        reason: TransactionReason,
        reference_id: UUID | None = None,
    ) -> UUID:
        """Вернуть кредиты пользователю после отменённой или частичной операции."""
        if amount <= 0:
            raise ValueError("amount must be > 0")
        user = self._get_user(user_id)
        return self.add_credit(
            UUID(user.organization_id),
            user_id,
            amount,
            reason,
            reference_id,
        )

    def get_balance(self, organization_id: UUID) -> Decimal:
        """Вернуть текущий баланс организации, создавая нулевое значение логически."""
        account = self._session.get(Account, str(organization_id))
        return account.balance if account is not None else Decimal("0.00")

    def should_skip_metered_withdrawal(self, user_id: UUID) -> bool:
        """Проверить, освобождена ли организация пользователя от списаний по лицензии."""
        user = self._get_user(user_id)
        organization = self._session.get(Organization, user.organization_id)
        if organization is None:
            raise UserAccountNotFoundError()
        if organization.license_type != LicenseType.ONPREMISE.value:
            return False
        return not _is_expired(organization.access_expires_at)

    def get_transaction_history(
        self,
        organization_id: UUID | None,
        limit: int | None = 50,
        offset: int = 0,
        reason: TransactionReason | None = None,
    ) -> list[Transaction]:
        """Вернуть историю транзакций с фильтром по организации и причине."""
        statement = select(Transaction).order_by(Transaction.timestamp.desc())
        if organization_id is not None:
            statement = statement.where(
                Transaction.organization_id == str(organization_id)
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


def _is_expired(expires_at) -> bool:
    if expires_at is None:
        return True
    if expires_at.tzinfo is None or expires_at.utcoffset() is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return expires_at <= datetime.now(UTC)
