from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel, Field

from accounting.models import TransactionReason
from accounting.service import AccountingService
from users.deps import CurrentUser, SessionDep, authenticate, ensure_admin

router = APIRouter(prefix="/v1/accounting", tags=["accounting"])

CurrentUserDep = Annotated[CurrentUser, Depends(authenticate)]


class AddCreditRequest(BaseModel):
    user_id: UUID
    amount: Decimal = Field(gt=0)


class TransactionIdResponse(BaseModel):
    transaction_id: UUID


class BalanceResponse(BaseModel):
    user_id: UUID
    balance: str


class TransactionResponse(BaseModel):
    id: UUID
    timestamp: datetime
    amount: str
    reason: str
    reference_id: UUID | None = None


def get_accounting_service(session: SessionDep) -> AccountingService:
    return AccountingService(session)


@router.post("/credits", response_model=TransactionIdResponse, status_code=status.HTTP_201_CREATED)
def add_credit(
    request: AddCreditRequest,
    current_user: CurrentUserDep,
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
) -> TransactionIdResponse:
    ensure_admin(current_user)
    transaction_id = accounting.add_credit(request.user_id, request.amount)
    return TransactionIdResponse(transaction_id=transaction_id)


@router.get("/me/balance", response_model=BalanceResponse)
def get_my_balance(
    current_user: CurrentUserDep,
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
) -> BalanceResponse:
    return BalanceResponse(
        user_id=current_user.id,
        balance=str(accounting.get_balance(current_user.id)),
    )


@router.get("/me/transactions", response_model=list[TransactionResponse])
def get_my_transactions(
    current_user: CurrentUserDep,
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    reason: TransactionReason | None = None,
) -> list[TransactionResponse]:
    transactions = accounting.get_transaction_history(current_user.id, limit, offset, reason)
    return [
        _to_response(transaction)
        for transaction in transactions
    ]


def _to_response(transaction) -> TransactionResponse:
    return TransactionResponse(
        id=UUID(transaction.id),
        timestamp=transaction.timestamp,
        amount=str(transaction.amount),
        reason=transaction.reason,
        reference_id=UUID(transaction.reference_id) if transaction.reference_id else None,
    )
