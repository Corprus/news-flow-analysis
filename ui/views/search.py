from __future__ import annotations

import html
from datetime import datetime, time

import streamlit as st

from api_client import ApiClient, ApiError
from auth import refresh_account
from config import MIN_NEWS_DATE, MOSCOW_TIMEZONE
from formatting import escape_markdown, format_search_date, format_search_result_summary


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
                    )
                elif status == "failed":
                    st.error(search.get("error") or "Не удалось выполнить поиск.")
                else:
                    st.info(f"Статус поиска: {status_label}")
    except ApiError as exc:
        st.error(str(exc))


def render_search_result(result: dict, *, key_prefix: str) -> None:
    clusters = result.get("clusters")
    if clusters is None:
        clusters = [
            {
                "cluster_id": item.get("cluster_id") or item.get("article_id"),
                "representative_title": item.get("title", ""),
                "article_count": 1,
                "significant_count": int(item.get("novelty_label") == "significant"),
                "items": [item],
            }
            for item in result.get("items", [])
        ]
    if not clusters:
        st.info("Подходящих новостей не найдено.")
        return

    for cluster_index, cluster in enumerate(clusters):
        title = cluster.get("representative_title") or "Без названия"
        article_count = cluster.get("article_count", len(cluster.get("items", [])))
        significant_count = cluster.get("significant_count", 0)
        label = escape_markdown(title)
        if significant_count:
            label += f' · [⭐](# "Количество важных публикаций") {significant_count}'
        label += f' · [📰](# "Общее количество публикаций") {article_count}'
        cluster_date = format_search_date(cluster.get("published_from"))
        if cluster_date:
            label = f"**{label}** {cluster_date}"
        with st.expander(
            label,
            expanded=cluster_index == 0,
            key=f"cluster-expander-{key_prefix}-{cluster_index}",
        ):
            items = cluster.get("items", [])
            show_all = st.checkbox(
                "Показать дубликаты и все совпадения",
                key=(
                    f"cluster-all-{key_prefix}-"
                    f"{cluster.get('cluster_id')}-{cluster_index}"
                ),
            )
            visible_items = (
                items
                if show_all
                else [
                    item
                    for item in items
                    if item.get("novelty_label") != "duplicate"
                ][:3]
            )
            for item_index, item in enumerate(visible_items):
                render_search_article(
                    item,
                    key_prefix=(
                        f"{key_prefix}-{cluster.get('cluster_id')}-"
                        f"{cluster_index}-{item_index}"
                    ),
                )
            hidden_count = len(items) - len(visible_items)
            if hidden_count > 0 and not show_all:
                st.caption(f"Скрыто публикаций: {hidden_count}")


def render_search_article(item: dict, *, key_prefix: str) -> None:
    title = html.escape(str(item.get("title") or "Без названия"))
    novelty_label = item.get("novelty_label")
    is_significant = novelty_label == "significant"
    details = [format_search_date(item.get("published_at"))]
    if item.get("score") is not None:
        details.append(f"релевантность {float(item['score']):.3f}")
    if item.get("p_significant") is not None:
        details.append(f"важность {float(item['p_significant']):.0%}")
    metadata = html.escape(" · ".join(detail for detail in details if detail))
    title_style = (
        "font-weight:700;color:#f0f2f6"
        if is_significant
        else "font-weight:500;color:#a6adb7"
    )
    marker = (
        " <span title='Значимая новость' "
        "style='color:#f5c542;font-size:1.15em'>★</span>"
        if is_significant
        else ""
    )
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
        text_color = "#d7dbe0" if is_significant else "#9299a3"
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
                st.rerun(scope="fragment")
        else:
            st.markdown(
                f"<div style='color:{text_color}'>{html.escape(article_text)}</div>",
                unsafe_allow_html=True,
            )

    url = item.get("url")
    if isinstance(url, str) and url.startswith(("http://", "https://")):
        st.markdown(f"[Открыть источник]({url})")
