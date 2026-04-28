from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from users.exceptions import InvalidAccessTokenError


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


@dataclass(frozen=True)
class AccessTokenHandler:
    secret: str
    ttl_minutes: int

    def create_access_token(self, user_id: UUID, role: str) -> str:
        now = datetime.now(UTC)
        payload = {
            "sub": str(user_id),
            "role": role,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=self.ttl_minutes)).timestamp()),
            "jti": str(uuid4()),
            "type": "access",
        }
        body = _b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        signature = self._sign(body)
        return f"{body}.{signature}"

    def verify_access_token(self, token: str) -> dict[str, Any]:
        try:
            body, signature = token.split(".", 1)
            expected_signature = self._sign(body)
            if not hmac.compare_digest(signature, expected_signature):
                raise InvalidAccessTokenError()

            payload = json.loads(_b64decode(body).decode("utf-8"))
            if payload.get("type") != "access":
                raise InvalidAccessTokenError()
            if int(payload["exp"]) < int(datetime.now(UTC).timestamp()):
                raise InvalidAccessTokenError()
            UUID(payload["sub"])
            return payload
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise InvalidAccessTokenError() from exc

    def _sign(self, body: str) -> str:
        return _b64encode(
            hmac.new(
                self.secret.encode("utf-8"),
                body.encode("ascii"),
                hashlib.sha256,
            ).digest()
        )
