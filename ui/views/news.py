from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import streamlit as st

from api_client import ApiClient, ApiError
from auth import refresh_account
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
from formatting import format_search_date


def render_news(client: ApiClient) -> None:
    st.header("Мои новости")
    my_news_tab, manual_tab, file_tab = st.tabs(
        ["Список", "Публикация новости", "Импорт новостей"]
    )
    with my_news_tab:
        render_my_news(client, show_header=False)
    with manual_tab:
        render_manual_news_form(client)
    with file_tab:
        render_news_file_import(client)


def render_manual_news_form(client: ApiClient) -> None:
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
                refresh_account(client)
                st.session_state["my_news_notice"] = (
                    "Новость сохранена и отправлена на публикацию."
                )
            else:
                st.session_state["my_news_notice"] = "Черновик сохранён."
            st.rerun()
        except ApiError as exc:
            st.error(str(exc))


def render_news_file_import(client: ApiClient) -> None:
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
                refresh_account(client)
                message += (
                    f". Отправлено на обработку: {result['published_count']}"
                )
            st.session_state["my_news_notice"] = message + "."
            st.rerun()
        except ApiError as exc:
            st.error(str(exc))


def render_my_news(client: ApiClient, *, show_header: bool = True) -> None:
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
        render_processing_my_news(client)
        return
    render_my_news_content(client, news)


@st.fragment(run_every=2)
def render_processing_my_news(client: ApiClient) -> None:
    try:
        news = client.list_news_history()
    except ApiError as exc:
        st.error(str(exc))
        return
    render_my_news_content(client, news)
    if not any(item.get("status") in {"pending", "processing"} for item in news):
        st.rerun()


@st.dialog("Безвозвратно удалить черновики?")
def confirm_draft_deletion(client: ApiClient, article_ids: list[str]) -> None:
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


STATUS_LABELS = {
    "not_started": "Не обработана",
    "pending": "В очереди",
    "processing": "Обрабатывается",
    "processed": "Готова",
    "error": "Ошибка",
}
NOVELTY_LABELS = {
    "significant": "Важная",
    "minor": "Второстепенная",
    "duplicate": "Дубликат",
}
MANUAL_LABEL_OPTIONS = {
    None: "Автоматически",
    **NOVELTY_LABELS,
}
MANUAL_LABEL_VALUES = {
    label: value for value, label in MANUAL_LABEL_OPTIONS.items()
}


def render_my_news_content(client: ApiClient, news: list[dict]) -> None:
    if not news:
        st.info("Вы пока не добавили ни одной новости.")
        return

    drafts = [item for item in news if item.get("visibility") == "draft"]
    published = [item for item in news if item.get("visibility") == "public"]
    archived = [item for item in news if item.get("visibility") == "archived"]

    render_drafts(client, drafts)
    render_published(client, published)
    render_archived(client, archived)


def render_drafts(client: ApiClient, drafts: list[dict]) -> None:
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
                    refresh_account(client)
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
                confirm_draft_deletion(client, selected_article_ids)
    else:
        st.caption("Черновиков нет.")


def render_published(client: ApiClient, published: list[dict]) -> None:
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
            novelty_label = NOVELTY_LABELS.get(
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
                    "Обработка": STATUS_LABELS.get(item.get("status"), item.get("status")),
                    "Тип модели": NOVELTY_LABELS.get(
                        item.get("model_novelty_label"),
                        (
                            "Возможный дубликат"
                            if item.get("possible_duplicate")
                            else item.get("model_novelty_label") or "—"
                        ),
                    ),
                    "Редакторская метка": MANUAL_LABEL_OPTIONS.get(
                        item.get("manual_novelty_label"),
                        "Автоматически",
                    ),
                    "Итоговый тип": novelty_label,
                    "Оценка модели, %": (
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
                "Оценка модели, %",
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
                "Оценка модели, %": st.column_config.NumberColumn(
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
                    options=list(MANUAL_LABEL_VALUES),
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
            new_value = MANUAL_LABEL_VALUES[selected_label]
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
                    refresh_account(client)
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


def render_archived(client: ApiClient, archived: list[dict]) -> None:
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
                "Тип": NOVELTY_LABELS.get(
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
