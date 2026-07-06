"""Настройки pytest, которые делают CI-отчёт понятнее для проверки диплома."""

from __future__ import annotations

import re

import pytest

_PARAMETRIZED_SUFFIX_RE = re.compile(r"\[.*\]$")


def _description_from_test_name(item: pytest.Item) -> str:
    """Собрать безопасное для CI описание из имени тестовой функции."""
    name = getattr(item, "originalname", None) or item.name
    name = _PARAMETRIZED_SUFFIX_RE.sub("", name)
    if name.startswith("test_"):
        name = name.removeprefix("test_")
    return name.replace("_", " ")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Добавить описания сценариев в verbose-вывод pytest и JUnit."""
    for item in items:
        description = _description_from_test_name(item)
        if not description:
            continue
        item.user_properties.append(("description", description))
        item._nodeid = f"{item.nodeid} -- {description}"
