from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select

from accounting.models import TransactionReason
from accounting.service import AccountingService
from news.models import NewsArticle
from users.deps import CurrentUser, SessionDep, authenticate, ensure_admin

router = APIRouter(prefix="/v1/accounting", tags=["accounting"])

CurrentUserDep = Annotated[CurrentUser, Depends(authenticate)]


class AddCreditRequest(BaseModel):
    organization_id: UUID
    amount: Decimal = Field(gt=0)


class TransactionIdResponse(BaseModel):
    transaction_id: UUID


class BalanceResponse(BaseModel):
    organization_id: UUID
    balance: str


class TransactionResponse(BaseModel):
    id: UUID
    organization_id: UUID
    actor_user_id: UUID | None
    timestamp: datetime
    amount: str
    reason: str
    reference_id: UUID | None = None
    reference_title: str | None = None
    reference_url: str | None = None
    batch_id: UUID | None = None
    item_count: int = 1


def get_accounting_service(session: SessionDep) -> AccountingService:
    return AccountingService(session)


@router.post("/credits", response_model=TransactionIdResponse, status_code=status.HTTP_201_CREATED)
def add_credit(
    request: AddCreditRequest,
    current_user: CurrentUserDep,
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
) -> TransactionIdResponse:
    ensure_admin(current_user)
    transaction_id = accounting.add_credit(
        request.organization_id,
        current_user.id,
        request.amount,
    )
    return TransactionIdResponse(transaction_id=transaction_id)


@router.get("/me/balance", response_model=BalanceResponse)
def get_my_balance(
    current_user: CurrentUserDep,
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
) -> BalanceResponse:
    return BalanceResponse(
        organization_id=current_user.organization_id,
        balance=str(accounting.get_balance(current_user.organization_id)),
    )


@router.get("/me/transactions", response_model=list[TransactionResponse])
def get_my_transactions(
    current_user: CurrentUserDep,
    session: SessionDep,
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    reason: TransactionReason | None = None,
) -> list[TransactionResponse]:
    transactions = accounting.get_transaction_history(
        current_user.organization_id,
        limit=None,
        reason=reason,
    )
    article_ids = [
        transaction.reference_id
        for transaction in transactions
        if transaction.reason
        in {
            TransactionReason.NEWS_ADD.value,
            TransactionReason.NEWS_REPROCESS.value,
        }
        and transaction.reference_id is not None
    ]
    articles = (
        session.execute(
            select(NewsArticle).where(NewsArticle.id.in_(article_ids))
        ).scalars()
        if article_ids
        else []
    )
    article_by_id = {article.id: article for article in articles}
    grouped: dict[str, list] = {}
    group_order: list[str] = []
    for transaction in transactions:
        group_key = transaction.batch_id or transaction.id
        if group_key not in grouped:
            grouped[group_key] = []
            group_order.append(group_key)
        grouped[group_key].append(transaction)

    operations = [
        _to_response_group(
            grouped[group_key],
            article_by_id,
        )
        for group_key in group_order[offset : offset + limit]
    ]
    return operations


def _to_response(
    transaction,
    article: NewsArticle | None = None,
) -> TransactionResponse:
    return TransactionResponse(
        id=UUID(transaction.id),
        organization_id=UUID(transaction.organization_id),
        actor_user_id=UUID(transaction.actor_user_id) if transaction.actor_user_id else None,
        timestamp=transaction.timestamp,
        amount=str(transaction.amount),
        reason=transaction.reason,
        reference_id=UUID(transaction.reference_id) if transaction.reference_id else None,
        reference_title=article.title if article else None,
        reference_url=article.url if article else None,
        batch_id=UUID(transaction.batch_id) if transaction.batch_id else None,
    )


def _to_response_group(
    transactions: list,
    article_by_id: dict[str, NewsArticle],
) -> TransactionResponse:
    first = transactions[0]
    if first.batch_id is None or len(transactions) == 1:
        return _to_response(first, article_by_id.get(first.reference_id))

    return TransactionResponse(
        id=UUID(first.batch_id),
        organization_id=UUID(first.organization_id),
        actor_user_id=UUID(first.actor_user_id) if first.actor_user_id else None,
        timestamp=max(transaction.timestamp for transaction in transactions),
        amount=str(sum(transaction.amount for transaction in transactions)),
        reason=first.reason,
        batch_id=UUID(first.batch_id),
        item_count=len(transactions),
    )
