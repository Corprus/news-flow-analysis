from uuid import uuid4

from accounting.models import TransactionReason
from news.routes import (
    _prepay_import_publication_or_raise,
    _refund_import_prepayment_or_raise,
    _withdraw_or_raise,
)


class AccountingSpy:
    def __init__(self) -> None:
        self.withdraw_calls = []
        self.refund_calls = []

    def withdraw_credit(self, *args) -> None:
        self.withdraw_calls.append(args)

    def refund_credit(self, *args, **kwargs) -> None:
        self.refund_calls.append((args, kwargs))


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


def test_import_publication_prepayment_withdraws_once_for_expected_rows() -> None:
    accounting = AccountingSpy()
    user_id = uuid4()
    import_job_id = uuid4()

    prepayment = _prepay_import_publication_or_raise(
        accounting=accounting,
        user_id=user_id,
        amount_per_article=2,
        expected_count=3,
        import_job_id=import_job_id,
    )

    assert prepayment is not None
    assert prepayment.expected_count == 3
    assert accounting.withdraw_calls == [
        (
            user_id,
            6,
            TransactionReason.NEWS_ADD,
            import_job_id,
            import_job_id,
        )
    ]


def test_import_publication_refund_returns_only_unused_prepayment() -> None:
    accounting = AccountingSpy()
    user_id = uuid4()
    import_job_id = uuid4()
    prepayment = _prepay_import_publication_or_raise(
        accounting=accounting,
        user_id=user_id,
        amount_per_article=2,
        expected_count=5,
        import_job_id=import_job_id,
    )
    assert prepayment is not None
    prepayment.published_count = 3

    _refund_import_prepayment_or_raise(
        accounting=accounting,
        user_id=user_id,
        amount_per_article=2,
        prepayment=prepayment,
    )
    _refund_import_prepayment_or_raise(
        accounting=accounting,
        user_id=user_id,
        amount_per_article=2,
        prepayment=prepayment,
    )

    assert accounting.refund_calls == [
        (
            (user_id, 4),
            {
                "reason": TransactionReason.NEWS_IMPORT_REFUND,
                "reference_id": import_job_id,
            },
        )
    ]
