"""Настройки pytest, которые делают CI-отчёт понятнее для проверки диплома."""

from __future__ import annotations

import inspect

import pytest


def _first_docstring_line(item: pytest.Item) -> str | None:
    """Вернуть первую строку docstring теста, если она задана."""
    test_object = getattr(item, "obj", None)
    docstring = inspect.getdoc(test_object)
    if not docstring:
        return None
    return docstring.splitlines()[0].strip()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Добавить русские описания сценариев в verbose-вывод pytest и JUnit."""
    for item in items:
        description = _first_docstring_line(item)
        if not description:
            continue
        item.user_properties.append(("description", description))
        item._nodeid = f"{item.nodeid} -- {description}"
