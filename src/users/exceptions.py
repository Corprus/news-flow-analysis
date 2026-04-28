class UserManagementError(Exception):
    pass


class UserAlreadyExistsError(UserManagementError):
    pass


class UserNotFoundError(UserManagementError):
    pass


class InvalidCredentialsError(UserManagementError):
    pass


class InvalidAccessTokenError(UserManagementError):
    pass
