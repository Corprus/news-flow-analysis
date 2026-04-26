from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def resolve_model_source(value: str) -> str:
    path = Path(value).expanduser()
    if path.suffix.lower() != ".json" or not path.exists():
        return value

    with path.open(encoding="utf-8") as file:
        metadata = json.load(file)
    if not isinstance(metadata, dict):
        raise TypeError(f"Expected model registry JSON object in {path}")

    local_path = _string_or_none(metadata.get("local_model_path"))
    if local_path and Path(local_path).expanduser().exists():
        return local_path

    repo_id = _required_string(metadata, "repo_id")
    revision = _string_or_none(metadata.get("model_revision")) or _string_or_none(
        metadata.get("commit_hash")
    )
    if revision:
        return f"{repo_id}@{revision}"
    return repo_id


def _required_string(metadata: dict[str, Any], key: str) -> str:
    value = metadata.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Model registry field {key!r} must be a non-empty string")
    return value


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
