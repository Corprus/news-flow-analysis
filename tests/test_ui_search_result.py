import sys
from pathlib import Path
from types import SimpleNamespace

UI_DIR = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI_DIR))

sys.modules.setdefault(
    "streamlit",
    SimpleNamespace(
        fragment=lambda **_kwargs: (lambda function: function),
        session_state={},
    ),
)

import views.search as search_view  # noqa: E402
from views.search import (  # noqa: E402
    _format_hidden_item_count,
    _has_duplicate_items,
    _has_overflow_items,
    _hidden_cluster_summary,
    _novelty_label_text,
    _novelty_marker_html,
    _novelty_text_color,
    _novelty_title_style,
    _order_cluster_items,
    _search_result_legend_text,
    _should_show_search_history_legend,
    _visible_cluster_items,
    render_search_article,
)


def test_cluster_preview_hides_duplicates_and_overflow_items() -> None:
    """Поиск по умолчанию показывает первые три не-дубликатные публикации."""
    items = [
        {"article_id": "important", "novelty_label": "significant"},
        {"article_id": "duplicate", "novelty_label": "duplicate"},
        {"article_id": "minor-1", "novelty_label": "minor"},
        {"article_id": "minor-2", "novelty_label": "minor"},
        {"article_id": "minor-3", "novelty_label": "minor"},
    ]

    visible = _visible_cluster_items(
        items,
        hide_duplicates=True,
        show_all_matches=False,
    )

    assert [item["article_id"] for item in visible] == [
        "important",
        "minor-1",
        "minor-2",
    ]
    assert _hidden_cluster_summary(items, visible) == (
        "Скрыто: 1 повтор, 1 другое совпадение."
    )


def test_cluster_preview_can_show_duplicates_without_overflow_items() -> None:
    """Отключение фильтра повторов не раскрывает хвост списка."""
    items = [
        {"article_id": "important", "novelty_label": "significant"},
        {"article_id": "duplicate", "novelty_label": "duplicate"},
        {"article_id": "minor-1", "novelty_label": "minor"},
        {"article_id": "minor-2", "novelty_label": "minor"},
    ]

    visible = _visible_cluster_items(
        items,
        hide_duplicates=False,
        show_all_matches=False,
    )

    assert [item["article_id"] for item in visible] == [
        "important",
        "duplicate",
        "minor-1",
    ]
    assert _hidden_cluster_summary(items, visible) == (
        "Скрыто: 1 другое совпадение."
    )


def test_cluster_preview_can_show_overflow_without_duplicates() -> None:
    """Показ остальных совпадений можно включить, оставив повторы скрытыми."""
    items = [
        {"article_id": "important", "novelty_label": "significant"},
        {"article_id": "duplicate", "novelty_label": "duplicate"},
        {"article_id": "minor-1", "novelty_label": "minor"},
        {"article_id": "minor-2", "novelty_label": "minor"},
    ]

    visible = _visible_cluster_items(
        items,
        hide_duplicates=True,
        show_all_matches=True,
    )

    assert [item["article_id"] for item in visible] == [
        "important",
        "minor-1",
        "minor-2",
    ]
    assert _hidden_cluster_summary(items, visible) == (
        "Скрыто: 1 повтор."
    )


def test_cluster_preview_shows_everything_when_requested() -> None:
    """Отключение обоих ограничений раскрывает все публикации кластера."""
    items = [
        {"article_id": "important", "novelty_label": "significant"},
        {"article_id": "duplicate", "novelty_label": "duplicate"},
    ]

    visible = _visible_cluster_items(
        items,
        hide_duplicates=False,
        show_all_matches=True,
    )

    assert visible == items
    assert _hidden_cluster_summary(items, visible) == ""


def test_filter_controls_are_hidden_when_they_do_not_affect_results() -> None:
    """Чекбоксы поиска показываются только при наличии скрываемых элементов."""
    short_items = [
        {"article_id": "important", "novelty_label": "significant"},
        {"article_id": "minor", "novelty_label": "minor"},
    ]
    duplicate_only_items = [
        {"article_id": "important", "novelty_label": "significant"},
        {"article_id": "duplicate", "novelty_label": "duplicate"},
    ]
    overflow_items = [
        {"article_id": f"minor-{index}", "novelty_label": "minor"}
        for index in range(4)
    ]

    assert not _has_duplicate_items(short_items)
    assert not _has_overflow_items(short_items, hide_duplicates=True)
    assert _has_duplicate_items(duplicate_only_items)
    assert not _has_overflow_items(duplicate_only_items, hide_duplicates=True)
    assert not _has_duplicate_items(overflow_items)
    assert _has_overflow_items(overflow_items, hide_duplicates=True)


def test_novelty_label_text_explains_gray_search_items() -> None:
    """Метки модели переводятся в понятные подписи карточек поиска."""
    assert _novelty_label_text("significant") == "важная"
    assert _novelty_label_text("minor") == "фоновое совпадение"
    assert _novelty_label_text("duplicate") == "повтор"


def test_duplicate_search_item_has_separate_visual_marker() -> None:
    """Повторы в результатах поиска визуально отличаются от фоновых совпадений."""
    duplicate_marker = _novelty_marker_html("duplicate")

    assert "🔁" in duplicate_marker
    assert "Повтор уже известной публикации" in duplicate_marker
    assert _novelty_marker_html("minor") == ""
    assert _novelty_title_style("duplicate") != _novelty_title_style("minor")
    assert _novelty_text_color("duplicate") != _novelty_text_color("minor")


def test_search_result_legend_explains_markers_once() -> None:
    """Общая легенда результатов поиска объясняет маркеры и режим показа."""
    legend = _search_result_legend_text()

    assert "⭐ важная публикация" in legend
    assert "🔁 повтор" in legend
    assert "повторы скрыты" in legend


def test_search_history_legend_is_shown_only_for_done_results() -> None:
    """Легенда истории поиска нужна только при наличии готовых результатов."""
    assert _should_show_search_history_legend(
        [{"status": "done", "result": {"clusters": [{}]}}],
    )
    assert not _should_show_search_history_legend(
        [{"status": "queued", "result": {"clusters": [{}]}}],
    )
    assert not _should_show_search_history_legend(
        [{"status": "done", "result": {}}],
    )


def test_hidden_item_count_uses_readable_russian_plural_forms() -> None:
    """Подпись скрытых результатов использует естественные русские формы."""
    assert _format_hidden_item_count(2, "повтор") == "2 повтора"
    assert _format_hidden_item_count(5, "повтор") == "5 повторов"
    assert _format_hidden_item_count(2, "другое совпадение") == (
        "2 других совпадения"
    )


def test_cluster_items_can_be_ordered_from_newest_to_oldest() -> None:
    """Результаты обычного поиска можно показывать в обратной хронологии."""
    items = [
        {"article_id": "old", "published_at": "2020-01-01T00:00:00+00:00"},
        {"article_id": "without-date"},
        {"article_id": "new", "published_at": "2020-01-03T00:00:00+00:00"},
        {"article_id": "bad-date", "published_at": "not-a-date"},
    ]

    ordered_items = _order_cluster_items(
        items,
        chronological_order="newest_first",
    )

    assert [item["article_id"] for item in ordered_items] == [
        "new",
        "old",
        "without-date",
        "bad-date",
    ]
    assert [item["article_id"] for item in items] == [
        "old",
        "without-date",
        "new",
        "bad-date",
    ]


def test_search_article_read_more_uses_page_rerun(monkeypatch) -> None:
    """Раскрытие текста работает и на обычных страницах без fragment rerun."""
    rerun_calls = []
    session_state = {}
    fake_streamlit = SimpleNamespace(
        session_state=session_state,
        markdown=lambda *args, **kwargs: None,
        button=lambda *args, **kwargs: True,
        rerun=lambda *args, **kwargs: rerun_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(search_view, "st", fake_streamlit)

    render_search_article(
        {
            "article_id": "article-1",
            "title": "Новость",
            "summary": "Очень длинный текст новости " * 30,
            "published_at": "2020-01-01T00:00:00+03:00",
        },
        key_prefix="date-feed",
    )

    assert session_state["article-expanded-date-feed-article-1"] is True
    assert rerun_calls == [((), {})]
