import pytest
from ui.selection import merge_page_selection


def test_page_selection_persists_ids_from_other_pages() -> None:
    """Выбор черновиков сохраняет ID с других страниц и обновляет текущую."""
    result = merge_page_selection(
        {"old-page-id", "current-a"},
        ["current-a", "current-b"],
        [False, True],
    )

    assert result == ["current-b", "old-page-id"]


def test_page_selection_requires_matching_flags_count() -> None:
    """Несовпадение строк и флагов выбора считается ошибкой вызова."""
    with pytest.raises(ValueError):
        merge_page_selection({"old-page-id"}, ["current-a"], [])
