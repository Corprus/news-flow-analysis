from uuid import uuid4

from users.tokens import AccessTokenHandler


def test_access_token_contains_organization_id() -> None:
    handler = AccessTokenHandler(secret="test-secret", ttl_minutes=5)
    user_id = uuid4()
    organization_id = uuid4()

    token = handler.create_access_token(user_id, organization_id, "user")
    payload = handler.verify_access_token(token)

    assert payload["sub"] == str(user_id)
    assert payload["organization_id"] == str(organization_id)
