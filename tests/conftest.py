"""Настройки pytest, которые делают CI-отчёт понятнее для проверки диплома."""

from __future__ import annotations

import inspect

import pytest

_CYRILLIC_TO_ASCII = str.maketrans(
    {
        "А": "A",
        "Б": "B",
        "В": "V",
        "Г": "G",
        "Д": "D",
        "Е": "E",
        "Ё": "E",
        "Ж": "Zh",
        "З": "Z",
        "И": "I",
        "Й": "Y",
        "К": "K",
        "Л": "L",
        "М": "M",
        "Н": "N",
        "О": "O",
        "П": "P",
        "Р": "R",
        "С": "S",
        "Т": "T",
        "У": "U",
        "Ф": "F",
        "Х": "Kh",
        "Ц": "Ts",
        "Ч": "Ch",
        "Ш": "Sh",
        "Щ": "Sch",
        "Ъ": "",
        "Ы": "Y",
        "Ь": "",
        "Э": "E",
        "Ю": "Yu",
        "Я": "Ya",
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "g",
        "д": "d",
        "е": "e",
        "ё": "e",
        "ж": "zh",
        "з": "z",
        "и": "i",
        "й": "y",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "kh",
        "ц": "ts",
        "ч": "ch",
        "ш": "sh",
        "щ": "sch",
        "ъ": "",
        "ы": "y",
        "ь": "",
        "э": "e",
        "ю": "yu",
        "я": "ya",
    }
)


def _first_docstring_line(item: pytest.Item) -> str | None:
    """Вернуть первую строку docstring теста, если она задана."""
    test_object = getattr(item, "obj", None)
    docstring = inspect.getdoc(test_object)
    if not docstring:
        return None
    return docstring.splitlines()[0].strip()


def _ascii_for_ci(value: str) -> str:
    """Подготовить описание к выводу в GitHub Actions без риска mojibake."""
    transliterated = value.translate(_CYRILLIC_TO_ASCII)
    safe_chars = [
        char if 32 <= ord(char) < 127 else " "
        for char in transliterated
    ]
    return " ".join("".join(safe_chars).split())


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Добавить описания сценариев в verbose-вывод pytest и JUnit."""
    for item in items:
        description = _first_docstring_line(item)
        if not description:
            continue
        ci_description = _ascii_for_ci(description)
        item.user_properties.append(("description", ci_description))
        item._nodeid = f"{item.nodeid} -- {ci_description}"
