from uuid import uuid4

from accounting.models import TransactionReason
from news.routes import _withdraw_or_raise


class AccountingSpy:
    def __init__(self) -> None:
        self.withdraw_calls = []

    def withdraw_credit(self, *args) -> None:
        self.withdraw_calls.append(args)


def test_zero_cost_operation_does_not_create_financial_activity() -> None:
    accounting = AccountingSpy()

    _withdraw_or_raise(
        accounting=accounting,
        user_id=uuid4(),
        amount=0,
        reason=TransactionReason.NEWS_SEARCH,
        reference_id=uuid4(),
    )

    assert accounting.withdraw_calls == []
