from __future__ import annotations

import html
from datetime import datetime, time
from typing import Literal

import streamlit as st

from api_client import ApiClient, ApiError, is_authentication_error
from auth import refresh_account
from config import MIN_NEWS_DATE, MOSCOW_TIMEZONE
from formatting import (
    display_news_title,
    display_news_url,
    escape_markdown,
    format_search_date,
    format_search_result_summary,
)

ChronologicalOrder = Literal["source", "oldest_first", "newest_first"]


def render_search(client: ApiClient) -> None:
    st.header("Поиск новостей")
    with st.form("search_form"):
        query_text = st.text_input(
            "Что найти?",
            placeholder="Например: новости Санкт-Петербурга",
        )
        with st.expander("Дополнительные параметры"):
            col_topk, col_lang, col_relevance, col_novelty = st.columns(4)
            top_k = col_topk.number_input(
                "Количество результатов",
                min_value=1,
                max_value=100,
                value=20,
            )
            language_label = col_lang.selectbox(
                "Язык",
                ["Любой", "Русский", "Английский"],
            )
            min_relevance = col_relevance.number_input(
                "Минимальная релевантность",
                min_value=0.0,
                max_value=1.0,
                value=0.4,
                step=0.05,
                help=(
                    "Результаты с меньшим сходством будут исключены. "
                    "Для текущей модели значения ниже 0.4 обычно нерелевантны."
                ),
            )
            min_novelty = col_novelty.number_input(
                "Минимальная новизна",
                min_value=0.0,
                max_value=1.0,
                value=0.0,
                step=0.05,
                help="0 — без ограничения; 1 — только максимально новые материалы.",
            )

            st.caption("Период публикации (московское время)")
            col_from, col_to = st.columns(2)
            with col_from:
                published_from_date = st.date_input(
                    "Дата начала",
                    value=None,
                    min_value=MIN_NEWS_DATE,
                    format="DD.MM.YYYY",
                )
                published_from_time = st.time_input(
                    "Время начала",
                    value=time.min,
                    step=1800,
                )
            with col_to:
                published_to_date = st.date_input(
                    "Дата окончания",
                    value=None,
                    min_value=MIN_NEWS_DATE,
                    format="DD.MM.YYYY",
                )
                published_to_time = st.time_input(
                    "Время окончания",
                    value=time.max.replace(microsecond=0),
                    step=1800,
                )
        submitted = st.form_submit_button("Найти", type="primary")

    if submitted:
        query_text = query_text.strip()
        if not query_text:
            st.warning(
                "Введите текст запроса. Для просмотра новостей по дате "
                "используйте календарь слева."
            )
            render_search_history(client)
            return

        published_from = (
            datetime.combine(
                published_from_date,
                published_from_time,
                tzinfo=MOSCOW_TIMEZONE,
            )
            if published_from_date
            else None
        )
        published_to = (
            datetime.combine(
                published_to_date,
                published_to_time,
                tzinfo=MOSCOW_TIMEZONE,
            )
            if published_to_date
            else None
        )
        if published_from and published_to and published_to < published_from:
            st.warning("Дата окончания не может быть раньше даты начала.")
            return

        language = {
            "Русский": "ru",
            "Английский": "en",
        }.get(language_label)
        payload = {
            "query_text": query_text,
            "top_k": int(top_k),
            "language": language,
            "published_from": published_from.isoformat() if published_from else None,
            "published_to": published_to.isoformat() if published_to else None,
            "min_relevance": float(min_relevance),
            "min_novelty_score": float(min_novelty) if min_novelty > 0 else None,
        }
        try:
            client.search_news(payload)
            refresh_account(client)
            st.toast(
                "Поиск запущен. Результаты появятся в истории ниже.",
                icon="🔎",
            )
        except ApiError as exc:
            if is_authentication_error(exc):
                raise
            st.error(str(exc))

    render_search_history(client)


@st.fragment(run_every=2)
def render_search_history(client: ApiClient) -> None:
    st.subheader("История поиска")
    try:
        history = client.list_search_history()
        if not history:
            st.info("Поисковых запросов пока нет.")
            return
        if _should_show_search_history_legend(history):
            st.caption(_search_result_legend_text())
        for search in history:
            status = search.get("status", "unknown")
            status_label = {
                "queued": "в очереди",
                "processing": "выполняется",
                "done": "готово",
                "failed": "ошибка",
            }.get(status, status)
            query_text = search.get("query_text", "")
            created_at = format_search_date(search.get("created_at"))
            result = search.get("result") or {}
            result_summary = format_search_result_summary(result)
            label_parts = [query_text, status_label]
            if result_summary:
                label_parts.append(result_summary)
            search_label = f"**{' · '.join(label_parts)}** {created_at}"
            with st.expander(
                search_label,
                key=f"search-expander-{search.get('query_id')}",
            ):
                if status == "done":
                    render_search_result(
                        result,
                        key_prefix=str(search.get("query_id") or created_at),
                        chronological_order="newest_first",
                        show_legend=False,
                    )
                elif status == "failed":
                    st.error(search.get("error") or "Не удалось выполнить поиск.")
                else:
                    st.info(f"Статус поиска: {status_label}")
    except ApiError as exc:
        if is_authentication_error(exc):
            raise
        st.error(str(exc))


def render_search_result(
    result: dict,
    *,
    key_prefix: str,
    chronological_order: ChronologicalOrder = "source",
    show_legend: bool = True,
) -> None:
    """Показать найденные кластеры новостей с выбранным порядком публикаций внутри групп."""
    clusters = result.get("clusters")
    if clusters is None:
        clusters = [
            {
                "cluster_id": item.get("cluster_id") or item.get("article_id"),
                "representative_title": display_news_title(item.get("title"), ""),
                "article_count": 1,
                "significant_count": int(item.get("novelty_label") == "significant"),
                "duplicate_count": int(item.get("novelty_label") == "duplicate"),
                "items": [item],
            }
            for item in result.get("items", [])
        ]
    if not clusters:
        st.info("Подходящих новостей не найдено.")
        return

    if show_legend:
        st.caption(_search_result_legend_text())
    for cluster_index, cluster in enumerate(clusters):
        title = cluster.get("representative_title") or "Без названия"
        article_count = cluster.get("article_count", len(cluster.get("items", [])))
        significant_count = cluster.get("significant_count", 0)
        duplicate_count = cluster.get("duplicate_count", 0)
        label = escape_markdown(title)
        if significant_count:
            label += f' · [⭐](# "Количество важных публикаций") {significant_count}'
        if duplicate_count:
            label += f' · [🔁](# "Количество повторов") {duplicate_count}'
        label += f' · [📰](# "Общее количество публикаций") {article_count}'
        cluster_date = _format_cluster_period(cluster)
        if cluster_date:
            label = f"**{label}** {cluster_date}"
        with st.expander(
            label,
            expanded=False,
            key=f"cluster-expander-{key_prefix}-{cluster_index}",
        ):
            items = _order_cluster_items(
                cluster.get("items", []),
                chronological_order=chronological_order,
            )
            has_duplicates = _has_duplicate_items(items)
            has_overflow = _has_overflow_items(items, hide_duplicates=True)
            hide_duplicates = True
            show_all_matches = False
            control_count = int(has_overflow) + int(has_duplicates)
            if control_count:
                control_columns = st.columns(control_count)
                control_index = 0
                if has_overflow:
                    with control_columns[control_index]:
                        show_all_matches = st.checkbox(
                            "Показать остальные совпадения",
                            key=(
                                f"cluster-all-matches-{key_prefix}-"
                                f"{cluster.get('cluster_id')}-{cluster_index}"
                            ),
                        )
                    control_index += 1
                if has_duplicates:
                    with control_columns[control_index]:
                        hide_duplicates = st.checkbox(
                            "Скрыть повторы",
                            value=True,
                            key=(
                                f"cluster-hide-duplicates-{key_prefix}-"
                                f"{cluster.get('cluster_id')}-{cluster_index}"
                            ),
                        )
            visible_items = _visible_cluster_items(
                items,
                hide_duplicates=hide_duplicates,
                show_all_matches=show_all_matches,
            )
            for item_index, item in enumerate(visible_items):
                render_search_article(
                    item,
                    key_prefix=(
                        f"{key_prefix}-{cluster.get('cluster_id')}-"
                        f"{cluster_index}-{item_index}"
                    ),
                )
            hidden_summary = _hidden_cluster_summary(items, visible_items)
            if hidden_summary:
                st.caption(hidden_summary)


def _order_cluster_items(
    items: list[dict],
    *,
    chronological_order: ChronologicalOrder,
) -> list[dict]:
    """Вернуть публикации кластера в выбранном хронологическом порядке."""
    if chronological_order == "source":
        return items
    reverse = chronological_order == "newest_first"
    return sorted(items, key=_cluster_item_published_at, reverse=reverse)


def _cluster_item_published_at(item: dict) -> datetime:
    """Получить дату публикации для сортировки результатов поиска."""
    value = item.get("published_at")
    if not isinstance(value, str):
        return datetime.min.replace(tzinfo=MOSCOW_TIMEZONE)
    try:
        published_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=MOSCOW_TIMEZONE)
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        return published_at.replace(tzinfo=MOSCOW_TIMEZONE)
    return published_at.astimezone(MOSCOW_TIMEZONE)


def _format_cluster_period(cluster: dict) -> str:
    started = format_search_date(
        cluster.get("published_from"),
        hide_midnight=True,
    )
    finished = format_search_date(
        cluster.get("published_to"),
        hide_midnight=True,
    )
    if started and finished and started != finished:
        return f"{started} — {finished}"
    return started or finished


def render_search_article(item: dict, *, key_prefix: str) -> None:
    title = html.escape(display_news_title(item.get("title")))
    novelty_label = item.get("novelty_label")
    details = [
        format_search_date(
            item.get("published_at"),
            hide_midnight=True,
        )
    ]
    if item.get("score") is not None:
        details.append(f"релевантность {float(item['score']):.0%}")
    if item.get("p_significant") is not None:
        details.append(f"новизна {float(item['p_significant']):.0%}")
    details.append(_novelty_label_text(novelty_label))
    metadata = html.escape(" · ".join(detail for detail in details if detail))
    title_style = _novelty_title_style(novelty_label)
    marker = _novelty_marker_html(novelty_label)
    st.markdown(
        (
            f"<span style='{title_style}'>{title}</span>{marker} "
            f"<small style='color:#737b87'>{metadata}</small>"
        ),
        unsafe_allow_html=True,
    )

    article_text = str(item.get("summary") or item.get("content") or "").strip()
    if article_text:
        preview_limit = 350
        is_long = len(article_text) > preview_limit
        text_color = _novelty_text_color(novelty_label)
        if is_long:
            preview = article_text[:preview_limit].rsplit(" ", 1)[0]
            article_id = str(item.get("article_id"))
            state_key = f"article-expanded-{key_prefix}-{article_id}"
            is_expanded = st.session_state.get(state_key, False)
            visible_text = article_text if is_expanded else preview + "…"
            st.markdown(
                f"<div style='color:{text_color}'>{html.escape(visible_text)}</div>",
                unsafe_allow_html=True,
            )
            if st.button(
                "Свернуть ↑" if is_expanded else "Читать далее →",
                key=f"read-more-{key_prefix}-{article_id}",
                type="tertiary",
            ):
                st.session_state[state_key] = not is_expanded
                st.rerun()
        else:
            st.markdown(
                f"<div style='color:{text_color}'>{html.escape(article_text)}</div>",
                unsafe_allow_html=True,
            )

    url = display_news_url(item.get("url"))
    if isinstance(url, str) and url.startswith(("http://", "https://")):
        st.markdown(f"[Открыть источник]({url})")


def _visible_cluster_items(
    items: list[dict],
    *,
    hide_duplicates: bool,
    show_all_matches: bool,
) -> list[dict]:
    """Вернуть публикации кластера с учётом фильтра повторов и лимита показа."""
    filtered_items = [
        item
        for item in items
        if not hide_duplicates or item.get("novelty_label") != "duplicate"
    ]
    return filtered_items if show_all_matches else filtered_items[:3]


def _has_duplicate_items(items: list[dict]) -> bool:
    """Проверить, есть ли в кластере повторы."""
    return any(item.get("novelty_label") == "duplicate" for item in items)


def _has_overflow_items(items: list[dict], *, hide_duplicates: bool) -> bool:
    """Проверить, есть ли публикации сверх краткого предпросмотра."""
    return len(
        _visible_cluster_items(
            items,
            hide_duplicates=hide_duplicates,
            show_all_matches=True,
        )
    ) > 3


def _hidden_cluster_summary(
    items: list[dict],
    visible_items: list[dict],
) -> str:
    """Собрать понятную подпись о скрытых публикациях кластера."""
    visible_ids = {item.get("article_id") for item in visible_items}
    hidden_items = [
        item
        for item in items
        if item.get("article_id") not in visible_ids
    ]
    duplicate_count = sum(
        item.get("novelty_label") == "duplicate"
        for item in hidden_items
    )
    other_count = len(hidden_items) - duplicate_count
    parts = []
    if duplicate_count:
        parts.append(_format_hidden_item_count(duplicate_count, "повтор"))
    if other_count:
        parts.append(_format_hidden_item_count(other_count, "другое совпадение"))
    return f"Скрыто: {', '.join(parts)}." if parts else ""


def _format_hidden_item_count(count: int, singular: str) -> str:
    """Вернуть короткую подпись количества скрытых результатов поиска."""
    if singular == "повтор":
        forms = ("повтор", "повтора", "повторов")
    else:
        forms = ("другое совпадение", "других совпадения", "других совпадений")
    return f"{count} {_pluralize_ru(count, forms)}"


def _pluralize_ru(count: int, forms: tuple[str, str, str]) -> str:
    """Выбрать русскую форму существительного для количества."""
    value = abs(count) % 100
    if 11 <= value <= 14:
        return forms[2]
    value %= 10
    if value == 1:
        return forms[0]
    if 2 <= value <= 4:
        return forms[1]
    return forms[2]


def _novelty_label_text(novelty_label: str | None) -> str:
    """Перевести модельную метку новизны в подпись для UI."""
    return {
        "significant": "важная",
        "minor": "фоновое совпадение",
        "duplicate": "повтор",
    }.get(str(novelty_label or ""), "тип не определён")


def _search_result_legend_text() -> str:
    """Вернуть единую легенду для списка результатов поиска."""
    return (
        "⭐ важная публикация; 🔁 повтор; серым показаны фоновые совпадения. "
        "По умолчанию повторы скрыты, а список ограничен первыми тремя совпадениями."
    )


def _should_show_search_history_legend(history: list[dict]) -> bool:
    """Проверить, нужна ли легенда над историей поисковых запросов."""
    return any(search.get("status") == "done" and search.get("result") for search in history)


def _novelty_title_style(novelty_label: str | None) -> str:
    """Вернуть стиль заголовка публикации по метке новизны."""
    if novelty_label == "significant":
        return "font-weight:700;color:#f0f2f6"
    if novelty_label == "duplicate":
        return "font-weight:500;color:#7f8791;font-style:italic"
    return "font-weight:500;color:#a6adb7"


def _novelty_text_color(novelty_label: str | None) -> str:
    """Вернуть цвет текста публикации по метке новизны."""
    if novelty_label == "significant":
        return "#d7dbe0"
    if novelty_label == "duplicate":
        return "#7f8791"
    return "#9299a3"


def _novelty_marker_html(novelty_label: str | None) -> str:
    """Вернуть HTML-маркер типа публикации для карточки поиска."""
    if novelty_label == "significant":
        return (
            " <span title='Значимая новость' "
            "style='color:#f5c542;font-size:1.15em'>★</span>"
        )
    if novelty_label == "duplicate":
        return (
            " <span title='Повтор уже известной публикации' "
            "style='color:#8b949e;font-size:1.05em'>🔁</span>"
        )
    return ""
