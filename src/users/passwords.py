from __future__ import annotations

import base64
import hashlib
import hmac
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PasswordHasher:
    secret: str
    iterations: int = 210_000

    def hash(self, password: str) -> str:
        salt = os.urandom(16)
        digest = self._derive(password, salt)
        return (
            f"pbkdf2_sha256${self.iterations}$"
            f"{base64.urlsafe_b64encode(salt).decode()}$"
            f"{base64.urlsafe_b64encode(digest).decode()}"
        )

    def verify(self, password: str, password_hash: str) -> bool:
        try:
            algorithm, iterations, salt_b64, digest_b64 = password_hash.split("$", 3)
            if algorithm != "pbkdf2_sha256":
                return False
            salt = base64.urlsafe_b64decode(salt_b64.encode())
            expected = base64.urlsafe_b64decode(digest_b64.encode())
            actual = self._derive(password, salt, int(iterations))
            return hmac.compare_digest(actual, expected)
        except (ValueError, TypeError):
            return False

    def _derive(
        self,
        password: str,
        salt: bytes,
        iterations: int | None = None,
    ) -> bytes:
        return hashlib.pbkdf2_hmac(
            "sha256",
            f"{self.secret}:{password}".encode(),
            salt,
            iterations or self.iterations,
        )
