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
from formatting import display_news_title, display_news_url, format_search_date


def render_news(client: ApiClient) -> None:
    st.header("Мои новости")
    active_tab = st.radio(
        "Раздел новостей",
        ["Список", "Публикация новости", "Импорт новостей"],
        horizontal=True,
        label_visibility="collapsed",
        key="news-section",
    )
    if active_tab == "Список":
        render_my_news(client, show_header=False)
    elif active_tab == "Публикация новости":
        render_manual_news_form(client)
    else:
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
    active_import_job_id = st.session_state.get("news_import_job_id")
    if active_import_job_id:
        render_news_import_job_status(client, active_import_job_id)
        return
    try:
        latest_job = client.get_latest_news_import_job()
        latest_job_id = latest_job.get("import_job_id")
        ignored_job_id = st.session_state.get("news_import_ignored_job_id")
        if (
            latest_job.get("status") in {"queued", "processing"}
            and latest_job_id != ignored_job_id
        ):
            st.session_state["news_import_job_id"] = latest_job_id
            render_news_import_job_status(client, latest_job_id)
            return
    except ApiError as exc:
        if exc.status_code != 404:
            st.error(str(exc))
            return

    try:
        formats = client.list_news_import_formats()
    except ApiError as exc:
        st.error(str(exc))
        return
    if not formats:
        st.info("No file import formats are configured.")
        return

    format_by_label = {item["label"]: item for item in formats}
    file_col, format_col = st.columns([4, 1], vertical_alignment="top")
    with format_col:
        label = st.selectbox("Формат", list(format_by_label))
        selected_format = format_by_label[label]
    extensions = [
        extension.lstrip(".") for extension in selected_format.get("file_extensions", [])
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
    submitted = st.button(
        (
            "Загрузить и опубликовать"
            if publish_immediately
            else "Загрузить в черновики"
        ),
        type="primary",
    )

    if submitted:
        if uploaded_file is None:
            st.warning("Выберите файл для импорта.")
            return
        try:
            result = client.create_news_import_job(
                selected_format["id"],
                uploaded_file.name,
                uploaded_file.getvalue(),
                publish_immediately=publish_immediately,
            )
            st.session_state["news_import_job_id"] = result["import_job_id"]
            st.rerun()
        except ApiError as exc:
            st.error(str(exc))


@st.fragment(run_every=2)
def render_news_import_job_status(client: ApiClient, import_job_id: str) -> None:
    try:
        job = client.get_news_import_job(import_job_id)
    except ApiError as exc:
        st.error(str(exc))
        if st.button("Запустить новый импорт"):
            st.session_state.pop("news_import_job_id", None)
            st.session_state["news_import_ignored_job_id"] = import_job_id
            st.rerun()
        return

    status = job.get("status")
    result = job.get("result") or {}
    if status in {"queued", "processing"}:
        stage = {
            "queued": "ожидает запуска",
            "parsing": "чтение файла",
            "importing": "создание черновиков",
            "queueing_vectorization": "постановка на обработку",
        }.get(result.get("stage"), "выполняется")
        st.info(
            f"Импорт выполняется: {stage}. "
            "Для большого ZIP это может занять несколько минут."
        )
        st.progress(int(result.get("progress_percent") or 0))
        if result.get("file_name"):
            st.caption(f"Файл: {result['file_name']}")
        if result.get("total_rows") is not None:
            st.caption(f"Строк CSV найдено: {result['total_rows']}")
        if result.get("processed_rows") is not None:
            st.caption(
                f"Черновиков создано: {result['processed_rows']} из "
                f"{result.get('total_rows', '?')}"
            )
        if st.button("Скрыть этот статус и запустить новый импорт"):
            st.session_state.pop("news_import_job_id", None)
            st.session_state["news_import_ignored_job_id"] = import_job_id
            st.rerun()
        return

    st.session_state.pop("news_import_job_id", None)
    st.session_state["news_import_ignored_job_id"] = import_job_id
    if status == "failed":
        st.error(result.get("error") or "Импорт завершился с ошибкой.")
        return

    message = f"Импортировано новостей: {result.get('created_count', 0)}"
    if result.get("duplicate_count"):
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


def render_my_news(client: ApiClient, *, show_header: bool = True) -> None:
    if show_header:
        st.header("Мои новости")
    notice = st.session_state.pop("my_news_notice", None)
    if notice:
        st.toast(notice, icon="✅")

    render_processing_my_news(client)


@st.fragment(run_every=2)
def render_processing_my_news(client: ApiClient) -> None:
    render_my_news_content(client)


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
PROCESSING_STAGE_LABELS = {
    "queued_for_vectorization": "В очереди на векторизацию",
    "vectorization": "Векторизация",
    "clustering_and_novelty": "Кластеризация и новизна",
    "saving_result": "Сохранение результата",
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


def render_my_news_content(client: ApiClient) -> None:
    try:
        summary = client.get_news_history_summary()
        drafts, drafts_page, drafts_has_next = _load_history_page(client, "draft")
        processing, processing_page, processing_has_next = _load_history_page(
            client,
            "public_processing",
            visibility="public",
            statuses=["pending", "processing"],
        )
        published, published_page, published_has_next = _load_history_page(
            client,
            "public_ready",
            visibility="public",
            statuses=["processed", "error"],
        )
        archived, archived_page, archived_has_next = _load_history_page(client, "archived")
    except ApiError as exc:
        st.error(str(exc))
        return

    visibility_counts = summary.get("visibility_counts") or {}
    status_counts_by_visibility = summary.get("status_counts_by_visibility") or {}
    draft_total = int(visibility_counts.get("draft") or 0)
    published_total = int(visibility_counts.get("public") or 0)
    archived_total = int(visibility_counts.get("archived") or 0)
    public_status_counts = status_counts_by_visibility.get("public") or {}
    processing_total = sum(
        int(public_status_counts.get(status) or 0)
        for status in ("pending", "processing")
    )
    published_ready_total = max(published_total - processing_total, 0)
    published_ready_status_counts = {
        status: count
        for status, count in public_status_counts.items()
        if status in {"processed", "error"}
    }

    if draft_total == 0 and published_total == 0 and archived_total == 0:
        st.info("Вы пока не добавили ни одной новости.")
        return

    render_drafts(
        client,
        drafts,
        draft_total,
        page_key="draft",
        page=drafts_page,
        has_next=drafts_has_next,
    )
    render_processing_articles(
        processing,
        processing_total,
        page_key="public_processing",
        page=processing_page,
        has_next=processing_has_next,
    )
    render_published(
        client,
        published,
        published_ready_total,
        published_ready_status_counts,
        page_key="public_ready",
        page=published_page,
        has_next=published_has_next,
    )
    render_archived(
        client,
        archived,
        archived_total,
        page_key="archived",
        page=archived_page,
        has_next=archived_has_next,
    )


HISTORY_PAGE_SIZE = 100


def _load_history_page(
    client: ApiClient,
    page_key: str,
    *,
    visibility: str | None = None,
    statuses: list[str] | None = None,
) -> tuple[list[dict], int, bool]:
    resolved_visibility = visibility or page_key
    page = int(st.session_state.get(f"my-news-{page_key}-page", 0))
    items = client.list_news_history_page(
        visibility=resolved_visibility,
        statuses=statuses,
        limit=HISTORY_PAGE_SIZE,
        offset=page * HISTORY_PAGE_SIZE,
    )
    return items, page, len(items) == HISTORY_PAGE_SIZE


PAGER_COLUMN_WIDTH = 92


def render_table_with_pager(
    page_key: str,
    page: int,
    has_next: bool,
    total: int,
    render_table,
):
    with st.container(width=NEWS_TABLE_WIDTH + PAGER_COLUMN_WIDTH):
        table_col, pager_col = st.columns(
            [NEWS_TABLE_WIDTH, PAGER_COLUMN_WIDTH],
            gap="small",
            vertical_alignment="center",
        )
        with table_col:
            result = render_table()
        with pager_col:
            render_history_pager(page_key, page, has_next, total)
        return result


def render_history_pager(
    page_key: str,
    page: int,
    has_next: bool,
    total: int,
) -> None:
    if page == 0 and not has_next:
        return
    if st.button(
        "↑",
        key=f"my-news-{page_key}-previous",
        disabled=page <= 0,
        width="stretch",
        help="Предыдущая страница",
    ):
        st.session_state[f"my-news-{page_key}-page"] = max(page - 1, 0)
        st.rerun()
    page_count = max((total + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE, 1)
    st.caption(f"{page + 1}/{page_count}")
    st.caption(f"{total} всего")
    if st.button(
        "↓",
        key=f"my-news-{page_key}-next",
        disabled=not has_next,
        width="stretch",
        help="Следующая страница",
    ):
        st.session_state[f"my-news-{page_key}-page"] = page + 1
        st.rerun()


def render_drafts(
    client: ApiClient,
    drafts: list[dict],
    total: int,
    *,
    page_key: str,
    page: int,
    has_next: bool,
) -> None:
    render_history_section_header("Черновики", drafts, total)
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
                "Заголовок": display_news_title(item.get("title")),
                "Дата публикации": format_search_date(item.get("published_at")),
                "Проверка": (
                    "Возможный дубликат"
                    if item.get("possible_duplicate")
                    else "—"
                ),
                "Источник": display_news_url(item.get("url")),
            }
            for item in drafts
        ]
        edited_drafts = render_table_with_pager(
            page_key,
            page,
            has_next,
            total,
            lambda: st.data_editor(
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
            ),
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


def render_processing_articles(
    processing: list[dict],
    total: int,
    *,
    page_key: str,
    page: int,
    has_next: bool,
) -> None:
    render_history_section_header("В обработке", processing, total)
    if not processing:
        st.caption("Новостей в обработке нет.")
        return

    processing_rows = [
        {
            "Заголовок": display_news_title(item.get("title")),
            "Дата публикации": format_search_date(item.get("published_at")),
            "Этап": PROCESSING_STAGE_LABELS.get(
                item.get("processing_stage"),
                STATUS_LABELS.get(item.get("status"), item.get("status")),
            ),
            "Статус": STATUS_LABELS.get(item.get("status"), item.get("status")),
            "Источник": display_news_url(item.get("url")),
        }
        for item in processing
    ]
    render_table_with_pager(
        page_key,
        page,
        has_next,
        total,
        lambda: st.dataframe(
            pd.DataFrame(processing_rows),
            hide_index=True,
            width=NEWS_TABLE_WIDTH,
            column_config={
                "Заголовок": st.column_config.TextColumn(
                    "Заголовок",
                    width=PUBLISHED_TITLE_COLUMN_WIDTH,
                ),
                "Дата публикации": st.column_config.TextColumn(
                    "Дата публикации",
                    width=DATE_COLUMN_WIDTH,
                ),
                "Этап": st.column_config.TextColumn(
                    "Этап",
                    width=PUBLISHED_STATUS_COLUMN_WIDTH,
                ),
                "Статус": st.column_config.TextColumn(
                    "Статус",
                    width=PUBLISHED_STATUS_COLUMN_WIDTH,
                ),
                "Источник": st.column_config.LinkColumn(
                    "Источник",
                    display_text="Открыть",
                    width=SOURCE_COLUMN_WIDTH,
                ),
            },
        ),
    )


def render_published(
    client: ApiClient,
    published: list[dict],
    total: int,
    status_counts: dict,
    *,
    page_key: str,
    page: int,
    has_next: bool,
) -> None:
    render_history_section_header("Опубликованные", published, total)
    render_published_status_summary(status_counts)
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
                    "Заголовок": display_news_title(item.get("title")),
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
                    "Источник": display_news_url(item.get("url")),
                }
            )

        edited_published = render_table_with_pager(
            page_key,
            page,
            has_next,
            total,
            lambda: st.data_editor(
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
            ),
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


def render_archived(
    client: ApiClient,
    archived: list[dict],
    total: int,
    *,
    page_key: str,
    page: int,
    has_next: bool,
) -> None:
    render_history_section_header("Архивные", archived, total)
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
                "Заголовок": display_news_title(item.get("title")),
                "Дата публикации": format_search_date(item.get("published_at")),
                "Тип": NOVELTY_LABELS.get(
                    item.get("novelty_label"),
                    item.get("novelty_label") or "—",
                ),
                "Источник": display_news_url(item.get("url")),
            }
            for item in archived
        ]
        edited_archived = render_table_with_pager(
            page_key,
            page,
            has_next,
            total,
            lambda: st.data_editor(
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
            ),
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


def render_history_section_header(title: str, items: list[dict], total: int) -> None:
    st.subheader(f"{title} · {total} всего")
    if total:
        st.caption(f"Показано на странице: {len(items)}.")


def render_published_status_summary(status_counts: dict) -> None:
    if not status_counts:
        return
    parts = []
    for status, label in STATUS_LABELS.items():
        count = int(status_counts.get(status) or 0)
        if count:
            parts.append(f"{label}: {count}")
    if parts:
        st.caption(" · ".join(parts))
