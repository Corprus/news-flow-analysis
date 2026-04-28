class AccountingError(Exception):
    pass


class UserAccountNotFoundError(AccountingError):
    pass


class InsufficientBalanceError(AccountingError):
    pass
