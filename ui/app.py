from __future__ import annotations

import html
from datetime import date, datetime, time

import extra_streamlit_components as stx
import pandas as pd
import streamlit as st

from api_client import ApiError
from auth import (
    clear_authentication,
    clear_pending_auth_cookie,
    get_client,
    refresh_account,
    render_login,
)
from config import (
    ARCHIVE_TITLE_COLUMN_WIDTH,
    ARCHIVE_TYPE_COLUMN_WIDTH,
    DATE_COLUMN_WIDTH,
    DRAFT_REVIEW_COLUMN_WIDTH,
    DRAFT_TITLE_COLUMN_WIDTH,
    MOSCOW_TIMEZONE,
    NEWS_TABLE_WIDTH,
    PUBLISHED_EDITOR_LABEL_COLUMN_WIDTH,
    PUBLISHED_EFFECTIVE_TYPE_COLUMN_WIDTH,
    PUBLISHED_IMPORTANCE_COLUMN_WIDTH,
    PUBLISHED_MODEL_TYPE_COLUMN_WIDTH,
    PUBLISHED_STATUS_COLUMN_WIDTH,
    PUBLISHED_TITLE_COLUMN_WIDTH,
    SELECT_COLUMN_WIDTH,
    SOURCE_COLUMN_WIDTH,
)
from formatting import (
    escape_markdown,
    format_amount,
    format_search_date,
    format_search_result_summary,
    parse_decimal,
)
from navigation import render_sidebar
from styles import apply_styles

st.set_page_config(page_title="News Flow", layout="wide")
apply_styles()
cookie_manager = stx.CookieManager(key="auth_cookie_manager")
client = get_client(cookie_manager)
clear_pending_auth_cookie(cookie_manager)


def render_news() -> None:
    st.header("Мои новости")
    my_news_tab, manual_tab, file_tab = st.tabs(
        ["Список", "Публикация новости", "Импорт новостей"]
    )
    with my_news_tab:
        render_my_news(show_header=False)
    with manual_tab:
        render_manual_news_form()
    with file_tab:
        render_news_file_import()


def render_manual_news_form() -> None:
    manual_now = datetime.now(MOSCOW_TIMEZONE)
    use_current_time = st.checkbox(
        "Использовать текущие дату и время",
        value=True,
        key="manual-news-current-time",
    )
    with st.form("add_news_form"):
        title = st.text_input("Заголовок")
        content = st.text_area("Текст новости", height=260)
        col_url, col_lang = st.columns([3, 1])
        url = col_url.text_input("Ссылка на источник")
        language = col_lang.selectbox(
            "Язык",
            ["Русский", "Английский"],
        )
        st.caption("Дата публикации (московское время)")
        col_date, col_time = st.columns(2)
        published_date = col_date.date_input(
            "Дата",
            value=manual_now.date(),
            min_value=date(1900, 1, 1),
            max_value=date(2100, 12, 31),
            format="DD.MM.YYYY",
            disabled=use_current_time,
        )
        published_time = col_time.time_input(
            "Время",
            value=manual_now.time().replace(second=0, microsecond=0),
            step=60,
            disabled=use_current_time,
        )
        summary = st.text_area("Краткое описание", height=100)
        publish_immediately = st.checkbox("Опубликовать сразу")
        submitted = st.form_submit_button(
            "Сохранить и опубликовать"
            if publish_immediately
            else "Сохранить черновик",
            type="primary",
        )

    if submitted:
        published_at = (
            datetime.now(MOSCOW_TIMEZONE)
            if use_current_time
            else datetime.combine(
                published_date,
                published_time,
                tzinfo=MOSCOW_TIMEZONE,
            )
        )
        payload = {
            "title": title,
            "content": content,
            "summary": summary or None,
            "url": url or None,
            "canonical_url": url or None,
            "language": {"Русский": "ru", "Английский": "en"}[language],
            "published_at": published_at.isoformat(),
            "publish_immediately": publish_immediately,
        }
        try:
            result = client.add_news(payload)
            if result.get("job_id"):
                refresh_account()
                st.session_state["my_news_notice"] = (
                    "Новость сохранена и отправлена на публикацию."
                )
            else:
                st.session_state["my_news_notice"] = "Черновик сохранён."
            st.rerun()
        except ApiError as exc:
            st.error(str(exc))


def render_news_file_import() -> None:
    try:
        formats = client.list_news_import_formats()
    except ApiError as exc:
        st.error(str(exc))
        return
    if not formats:
        st.info("No file import formats are configured.")
        return

    format_by_label = {item["label"]: item for item in formats}
    with st.form("import_news_form"):
        file_col, format_col = st.columns([4, 1], vertical_alignment="top")
        with format_col:
            label = st.selectbox("Формат", list(format_by_label))
            selected_format = format_by_label[label]
        extensions = [
            extension.lstrip(".")
            for extension in selected_format.get("file_extensions", [])
        ]
        with file_col:
            uploaded_file = st.file_uploader(
                "Файл с новостями",
                type=extensions or None,
                key="import-news-file",
            )
        st.caption("Не более 200 МБ на файл")
        publish_immediately = st.checkbox(
            "Опубликовать сразу",
            key="import-publish-immediately",
        )
        submitted = st.form_submit_button(
            "Импортировать и опубликовать"
            if publish_immediately
            else "Импортировать черновики"
        )

    if submitted:
        if uploaded_file is None:
            st.warning("Выберите файл для импорта.")
            return
        try:
            result = client.import_news(
                selected_format["id"],
                uploaded_file.name,
                uploaded_file.getvalue(),
                publish_immediately=publish_immediately,
            )
            message = f"Импортировано новостей: {result['created_count']}"
            if result["duplicate_count"]:
                message += (
                    f". Возможных дубликатов: {result['duplicate_count']} — "
                    "они созданы отдельными записями"
                )
            if result.get("published_count"):
                refresh_account()
                message += (
                    f". Отправлено на обработку: {result['published_count']}"
                )
            st.session_state["my_news_notice"] = message + "."
            st.rerun()
        except ApiError as exc:
            st.error(str(exc))


def render_search() -> None:
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
                value=0.5,
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
                    format="DD.MM.YYYY",
                )
                published_to_time = st.time_input(
                    "Время окончания",
                    value=time.max.replace(microsecond=0),
                    step=1800,
                )
        submitted = st.form_submit_button("Найти", type="primary")

    if submitted:
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
            refresh_account()
            st.toast(
                "Поиск запущен. Результаты появятся в истории ниже.",
                icon="🔎",
            )
        except ApiError as exc:
            st.error(str(exc))

    render_search_history()


@st.fragment(run_every=2)
def render_search_history() -> None:
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
                "significant_count": int(
                    item.get("novelty_label") == "significant"
                ),
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
            label += (
                f' · [⭐](# "Количество важных публикаций") {significant_count}'
            )
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
    title_style = "font-weight:700;color:#f0f2f6" if is_significant else (
        "font-weight:500;color:#a6adb7"
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


def render_my_news(*, show_header: bool = True) -> None:
    if show_header:
        st.header("Мои новости")
    notice = st.session_state.pop("my_news_notice", None)
    if notice:
        st.toast(notice, icon="✅")

    try:
        news = client.list_news_history()
    except ApiError as exc:
        st.error(str(exc))
        return
    if any(item.get("status") in {"pending", "processing"} for item in news):
        render_processing_my_news()
        return
    render_my_news_content(news)


@st.fragment(run_every=2)
def render_processing_my_news() -> None:
    try:
        news = client.list_news_history()
    except ApiError as exc:
        st.error(str(exc))
        return
    render_my_news_content(news)
    if not any(item.get("status") in {"pending", "processing"} for item in news):
        st.rerun()


@st.dialog("Безвозвратно удалить черновики?")
def confirm_draft_deletion(article_ids: list[str]) -> None:
    count = len(article_ids)
    suffix = "черновик" if count == 1 else "черновика" if 2 <= count <= 4 else "черновиков"
    st.warning(
        f"Это безвозвратно удалит {count} {suffix}. "
        "Отменить это действие будет нельзя."
    )
    cancel_col, delete_col = st.columns(2)
    with cancel_col:
        if st.button("Отмена", width="stretch"):
            st.rerun()
    with delete_col:
        if st.button("Удалить", type="primary", width="stretch"):
            try:
                result = client.delete_news_drafts(article_ids)
                st.session_state["my_news_notice"] = (
                    f"Удалено черновиков: {result['deleted_count']}."
                )
                st.session_state["reset-my-news-drafts-select-all"] = True
                st.rerun()
            except ApiError as exc:
                st.error(str(exc))


def render_my_news_content(news: list[dict]) -> None:
    if not news:
        st.info("Вы пока не добавили ни одной новости.")
        return

    status_labels = {
        "not_started": "Не обработана",
        "pending": "В очереди",
        "processing": "Обрабатывается",
        "processed": "Готова",
        "error": "Ошибка",
    }
    novelty_labels = {
        "significant": "Важная",
        "minor": "Второстепенная",
        "duplicate": "Дубликат",
    }
    manual_label_options = {
        None: "Автоматически",
        **novelty_labels,
    }
    manual_label_values = {
        label: value for value, label in manual_label_options.items()
    }

    drafts = [item for item in news if item.get("visibility") == "draft"]
    published = [item for item in news if item.get("visibility") == "public"]
    archived = [item for item in news if item.get("visibility") == "archived"]

    st.subheader(f"Черновики · {len(drafts)}")
    if drafts:
        if st.session_state.pop("reset-my-news-drafts-select-all", False):
            st.session_state["my-news-drafts-select-all"] = False
        select_all_drafts = st.checkbox(
            "Выбрать все черновики",
            key="my-news-drafts-select-all",
        )
        draft_rows = [
            {
                "Выбрать": select_all_drafts,
                "Заголовок": item.get("title"),
                "Дата публикации": format_search_date(item.get("published_at")),
                "Проверка": (
                    "Возможный дубликат"
                    if item.get("possible_duplicate")
                    else "—"
                ),
                "Источник": item.get("url") or "",
            }
            for item in drafts
        ]
        edited_drafts = st.data_editor(
            pd.DataFrame(draft_rows),
            hide_index=True,
            width=NEWS_TABLE_WIDTH,
            disabled=["Заголовок", "Дата публикации", "Проверка", "Источник"],
            column_config={
                "Выбрать": st.column_config.CheckboxColumn(
                    "✓",
                    help="Отметьте черновики для публикации или удаления",
                    width=SELECT_COLUMN_WIDTH,
                ),
                "Заголовок": st.column_config.TextColumn(
                    "Заголовок",
                    width=DRAFT_TITLE_COLUMN_WIDTH,
                ),
                "Дата публикации": st.column_config.TextColumn(
                    "Дата публикации",
                    width=DATE_COLUMN_WIDTH,
                ),
                "Проверка": st.column_config.TextColumn(
                    "Проверка",
                    help=(
                        "Предварительное совпадение по ссылке или тексту. "
                        "Окончательный тип появится после обработки."
                    ),
                    width=DRAFT_REVIEW_COLUMN_WIDTH,
                ),
                "Источник": st.column_config.LinkColumn(
                    "Источник",
                    display_text="Открыть",
                    width=SOURCE_COLUMN_WIDTH,
                ),
            },
            key=f"my-news-drafts-editor-{int(select_all_drafts)}",
        )
        selected_article_ids = [
            drafts[index]["article_id"]
            for index, selected in enumerate(edited_drafts["Выбрать"].tolist())
            if selected
        ]
        publish_col, delete_col = st.columns(2)
        with publish_col:
            if st.button(
                "Опубликовать выбранные",
                disabled=not selected_article_ids,
                type="primary",
                width="stretch",
            ):
                try:
                    result = client.publish_news_batch(selected_article_ids)
                    refresh_account()
                    st.session_state["my_news_notice"] = (
                        f"Отправлено на публикацию: {result['published_count']}."
                    )
                    st.session_state["reset-my-news-drafts-select-all"] = True
                    st.rerun()
                except ApiError as exc:
                    st.error(str(exc))
        with delete_col:
            if st.button(
                "Удалить выбранные",
                disabled=not selected_article_ids,
                width="stretch",
            ):
                confirm_draft_deletion(selected_article_ids)
    else:
        st.caption("Черновиков нет.")

    st.subheader(f"Опубликованные · {len(published)}")
    if published:
        if st.session_state.pop("reset-my-news-published-select-all", False):
            st.session_state["my-news-published-select-all"] = False
        select_all_published = st.checkbox(
            "Выбрать все опубликованные",
            key="my-news-published-select-all",
        )
        published_rows = []
        for item in published:
            novelty_label = novelty_labels.get(
                item.get("novelty_label"),
                item.get("novelty_label") or "—",
            )
            if item.get("novelty_needs_review") and item.get("novelty_label"):
                novelty_label += " · пограничная оценка"
            published_rows.append(
                {
                    "Выбрать": select_all_published,
                    "Заголовок": item.get("title"),
                    "Дата публикации": format_search_date(item.get("published_at")),
                    "Обработка": status_labels.get(item.get("status"), item.get("status")),
                    "Тип модели": novelty_labels.get(
                        item.get("model_novelty_label"),
                        (
                            "Возможный дубликат"
                            if item.get("possible_duplicate")
                            else item.get("model_novelty_label") or "—"
                        ),
                    ),
                    "Редакторская метка": manual_label_options.get(
                        item.get("manual_novelty_label"),
                        "Автоматически",
                    ),
                    "Итоговый тип": novelty_label,
                    "Важность, %": (
                        round(float(item["novelty_score"]) * 100)
                        if item.get("novelty_score") is not None
                        else None
                    ),
                    "Источник": item.get("url") or "",
                }
            )

        edited_published = st.data_editor(
            pd.DataFrame(published_rows),
            hide_index=True,
            width=NEWS_TABLE_WIDTH,
            disabled=[
                "Заголовок",
                "Дата публикации",
                "Обработка",
                "Тип модели",
                "Итоговый тип",
                "Важность, %",
                "Источник",
            ],
            column_config={
                "Выбрать": st.column_config.CheckboxColumn(
                    "✓",
                    help="Отметьте новости для повторной обработки или архивирования",
                    width=SELECT_COLUMN_WIDTH,
                ),
                "Заголовок": st.column_config.TextColumn(
                    "Заголовок",
                    width=PUBLISHED_TITLE_COLUMN_WIDTH,
                ),
                "Дата публикации": st.column_config.TextColumn(
                    "Дата публикации",
                    width=DATE_COLUMN_WIDTH,
                ),
                "Обработка": st.column_config.TextColumn(
                    "Обработка",
                    width=PUBLISHED_STATUS_COLUMN_WIDTH,
                ),
                "Тип модели": st.column_config.TextColumn(
                    "Тип модели",
                    width=PUBLISHED_MODEL_TYPE_COLUMN_WIDTH,
                ),
                "Важность, %": st.column_config.NumberColumn(
                    help=(
                        "Автоматическая оценка модели: насколько вероятно, что "
                        "новость содержит важное обновление сюжета. "
                        "От 50% новость считается важной."
                    ),
                    min_value=0,
                    max_value=100,
                    format="%d%%",
                    width=PUBLISHED_IMPORTANCE_COLUMN_WIDTH,
                ),
                "Редакторская метка": st.column_config.SelectboxColumn(
                    "Редакторская метка",
                    help=(
                        "Заменяет результат модели в поиске. "
                        "«Автоматически» сбрасывает ручную коррекцию."
                    ),
                    options=list(manual_label_values),
                    required=True,
                    width=PUBLISHED_EDITOR_LABEL_COLUMN_WIDTH,
                ),
                "Итоговый тип": st.column_config.TextColumn(
                    "Итоговый тип",
                    width=PUBLISHED_EFFECTIVE_TYPE_COLUMN_WIDTH,
                ),
                "Источник": st.column_config.LinkColumn(
                    "Источник",
                    display_text="Открыть",
                    width=SOURCE_COLUMN_WIDTH,
                ),
            },
            key=f"my-news-published-editor-{int(select_all_published)}",
        )
        selected_published_ids = [
            published[index]["article_id"]
            for index, selected in enumerate(edited_published["Выбрать"].tolist())
            if selected
        ]
        label_updates = []
        for index, selected_label in enumerate(
            edited_published["Редакторская метка"].tolist()
        ):
            new_value = manual_label_values[selected_label]
            if new_value != published[index].get("manual_novelty_label"):
                label_updates.append(
                    {
                        "article_id": published[index]["article_id"],
                        "label": new_value,
                    }
                )

        save_col, reprocess_col, archive_col = st.columns([1, 1, 1])
        with save_col:
            if st.button(
                "Сохранить метки",
                disabled=not label_updates,
                width="stretch",
            ):
                try:
                    result = client.update_news_novelty_labels(label_updates)
                    st.session_state["my_news_notice"] = (
                        f"Сохранено редакторских меток: {result['updated_count']}."
                    )
                    st.rerun()
                except ApiError as exc:
                    st.error(str(exc))
        with reprocess_col:
            reprocessable_ids = [
                published[index]["article_id"]
                for index, selected in enumerate(
                    edited_published["Выбрать"].tolist()
                )
                if selected
                and published[index].get("status") in {"processed", "error"}
            ]
            if st.button(
                "Обработать повторно",
                disabled=not reprocessable_ids,
                width="stretch",
                help="Повторно выполнить кластеризацию и автоматическую разметку",
            ):
                try:
                    result = client.reprocess_news(reprocessable_ids)
                    refresh_account()
                    st.session_state["my_news_notice"] = (
                        f"Отправлено на повторную обработку: {result['queued_count']}."
                    )
                    st.session_state["reset-my-news-published-select-all"] = True
                    st.rerun()
                except ApiError as exc:
                    st.error(str(exc))
        with archive_col:
            if st.button(
                "Архивировать выбранные",
                disabled=not selected_published_ids,
                width="stretch",
            ):
                try:
                    result = client.archive_news(selected_published_ids)
                    st.session_state["my_news_notice"] = (
                        f"Архивировано новостей: {result['updated_count']}."
                    )
                    st.session_state["reset-my-news-published-select-all"] = True
                    st.rerun()
                except ApiError as exc:
                    st.error(str(exc))
    else:
        st.caption("Опубликованных новостей пока нет.")

    st.subheader(f"Архивные · {len(archived)}")
    if archived:
        if st.session_state.pop("reset-my-news-archived-select-all", False):
            st.session_state["my-news-archived-select-all"] = False
        select_all_archived = st.checkbox(
            "Выбрать все архивные",
            key="my-news-archived-select-all",
        )
        archived_rows = [
            {
                "Выбрать": select_all_archived,
                "Заголовок": item.get("title"),
                "Дата публикации": format_search_date(item.get("published_at")),
                "Тип": novelty_labels.get(
                    item.get("novelty_label"),
                    item.get("novelty_label") or "—",
                ),
                "Источник": item.get("url") or "",
            }
            for item in archived
        ]
        edited_archived = st.data_editor(
            pd.DataFrame(archived_rows),
            hide_index=True,
            width=NEWS_TABLE_WIDTH,
            disabled=["Заголовок", "Дата публикации", "Тип", "Источник"],
            column_config={
                "Выбрать": st.column_config.CheckboxColumn(
                    "✓",
                    help="Отметьте новости для возврата в публикацию",
                    width=SELECT_COLUMN_WIDTH,
                ),
                "Заголовок": st.column_config.TextColumn(
                    "Заголовок",
                    width=ARCHIVE_TITLE_COLUMN_WIDTH,
                ),
                "Дата публикации": st.column_config.TextColumn(
                    "Дата публикации",
                    width=DATE_COLUMN_WIDTH,
                ),
                "Тип": st.column_config.TextColumn(
                    "Тип",
                    width=ARCHIVE_TYPE_COLUMN_WIDTH,
                ),
                "Источник": st.column_config.LinkColumn(
                    "Источник",
                    display_text="Открыть",
                    width=SOURCE_COLUMN_WIDTH,
                ),
            },
            key=f"my-news-archived-editor-{int(select_all_archived)}",
        )
        selected_archived_ids = [
            archived[index]["article_id"]
            for index, selected in enumerate(edited_archived["Выбрать"].tolist())
            if selected
        ]
        if st.button(
            "Вернуть в публикацию",
            disabled=not selected_archived_ids,
        ):
            try:
                result = client.restore_news(selected_archived_ids)
                st.session_state["my_news_notice"] = (
                    f"Возвращено в публикацию: {result['updated_count']}."
                )
                st.session_state["reset-my-news-archived-select-all"] = True
                st.rerun()
            except ApiError as exc:
                st.error(str(exc))
    else:
        st.caption("Архивных новостей нет.")


def render_transactions() -> None:
    st.header("Операции")
    reason_labels = {
        "": "Все операции",
        "news_add": "Публикация новости",
        "news_reprocess": "Повторная обработка новости",
        "news_search": "Поиск новостей",
        "credit_add": "Пополнение баланса",
        "credit_withdraw": "Списание средств",
    }
    reason = st.selectbox(
        "Тип операции",
        list(reason_labels),
        format_func=reason_labels.get,
    )
    try:
        transactions = client.list_transactions(reason or None)
        if transactions:
            rows = [
                {
                    "Дата": format_search_date(item.get("timestamp")),
                    "Операция": reason_labels.get(
                        item.get("reason"),
                        item.get("reason"),
                    ),
                    "Новость": (
                        (
                            f"Пакетная повторная обработка: "
                            f"{item.get('item_count')} новостей"
                            if item.get("reason") == "news_reprocess"
                            else f"Пакетная публикация: {item.get('item_count')} новостей"
                        )
                        if item.get("batch_id") and item.get("item_count", 1) > 1
                        else item.get("reference_title") or "—"
                    ),
                    "Источник": item.get("reference_url") or "",
                    "Сумма": format_amount(item.get("amount", 0)),
                }
                for item in transactions
            ]
            st.dataframe(
                pd.DataFrame(rows),
                hide_index=True,
                width="stretch",
                column_config={
                    "Дата": st.column_config.TextColumn("Дата", width="small"),
                    "Операция": st.column_config.TextColumn(
                        "Операция",
                        width="medium",
                    ),
                    "Новость": st.column_config.TextColumn(
                        "Новость",
                        width="large",
                    ),
                    "Источник": st.column_config.LinkColumn(
                        "Источник",
                        display_text="Открыть",
                        width="small",
                    ),
                    "Сумма": st.column_config.TextColumn("Сумма", width="small"),
                },
            )
        else:
            st.info("Операций пока нет.")
    except ApiError as exc:
        st.error(str(exc))


def render_admin() -> None:
    st.header("Администрирование")
    try:
        users = client.list_users()
    except ApiError as exc:
        st.error(str(exc))
        return

    st.dataframe(pd.DataFrame(users), use_container_width=True, hide_index=True)
    with st.form("credit_form"):
        organization_id = st.text_input("Organization ID")
        amount_raw = st.text_input("Amount", value="10.00")
        submitted = st.form_submit_button("Add credits")
    if submitted:
        amount = parse_decimal(amount_raw)
        if amount is None:
            st.warning("Invalid amount")
            return
        try:
            client.add_credit(organization_id, amount)
            st.success("Credits added")
        except ApiError as exc:
            st.error(str(exc))


if not client.token:
    render_login(client, cookie_manager)
    if not client.token:
        st.stop()

if "me" not in st.session_state:
    try:
        refresh_account(client)
    except ApiError as exc:
        if exc.status_code in {401, 404}:
            clear_authentication(client)
            st.rerun()
        st.error(str(exc))
        st.stop()

page = render_sidebar(client)

if page == "Search":
    render_search()
elif page == "News":
    render_news()
elif page == "Transactions":
    render_transactions()
elif page == "Admin":
    render_admin()
