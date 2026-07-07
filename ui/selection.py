from __future__ import annotations


def merge_page_selection(
    selected_ids: set[str],
    page_ids: list[str],
    page_flags: list[bool],
) -> list[str]:
    """Синхронизировать отметки текущей страницы с общим выбором строк."""
    updated = selected_ids - set(page_ids)
    updated.update(
        row_id
        for row_id, selected in zip(page_ids, page_flags, strict=True)
        if selected
    )
    return sorted(updated)
