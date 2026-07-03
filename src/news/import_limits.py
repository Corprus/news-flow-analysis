from __future__ import annotations

import os

DEFAULT_NEWS_IMPORT_MAX_FILE_MIB = 512
DEFAULT_NEWS_IMPORT_MAX_ROWS = 1_000_000


def _read_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value == "":
        return default
    value = int(raw_value)
    if value < 1:
        raise ValueError(f"{name} must be greater than 0")
    return value


MAX_IMPORT_FILE_MIB = _read_int_env(
    "NEWS_IMPORT_MAX_FILE_MIB",
    DEFAULT_NEWS_IMPORT_MAX_FILE_MIB,
)
MAX_IMPORT_FILE_BYTES = MAX_IMPORT_FILE_MIB * 1024 * 1024
MAX_IMPORT_ROWS = _read_int_env(
    "NEWS_IMPORT_MAX_ROWS",
    DEFAULT_NEWS_IMPORT_MAX_ROWS,
)


def format_import_file_size_limit() -> str:
    return f"{MAX_IMPORT_FILE_MIB} MiB"
