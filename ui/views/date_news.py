from __future__ import annotations

from datetime import date, datetime, time, timedelta

import streamlit as st

from api_client import ApiClient, ApiError
from config import MIN_NEWS_DATE, MOSCOW_TIMEZONE
from views.search import render_search_result

PAGE_SIZE = 20


def render_date_news(client: ApiClient) -> None:
    st.header("Новости по дате")
    if "date_news_selected_date" not in st.session_state:
        try:
            latest = client.get_latest_news_date()
        except ApiError as exc:
            st.error(str(exc))
            return
        latest_date = _parse_api_date(latest.get("latest_date"))
        if latest_date is not None:
            st.session_state["date_news_selected_date"] = latest_date
            st.session_state["date_news_page"] = 0

    with st.form("date_news_form"):
        selected_date = st.date_input(
            "Дата публикации",
            value=st.session_state.get("date_news_selected_date"),
            min_value=MIN_NEWS_DATE,
            format="DD.MM.YYYY",
        )
        submitted = st.form_submit_button("Показать новости", type="primary")

    if submitted:
        st.session_state["date_news_selected_date"] = selected_date
        st.session_state["date_news_page"] = 0

    active_date = st.session_state.get("date_news_selected_date")
    if not isinstance(active_date, date):
        st.info("Выберите дату, чтобы посмотреть новостные сюжеты.")
        return

    render_date_news_result(client, active_date)


def render_date_news_result(client: ApiClient, selected_date: date) -> None:
    page = max(int(st.session_state.get("date_news_page", 0)), 0)
    published_from = datetime.combine(
        selected_date,
        time.min,
        tzinfo=MOSCOW_TIMEZONE,
    )
    published_to = published_from + timedelta(days=1)

    st.subheader(f"Новости за {selected_date:%d.%m.%Y}")
    try:
        adjacent_dates = client.get_adjacent_news_dates(
            published_from=published_from.isoformat(),
            published_to=published_to.isoformat(),
        )
        result = client.list_news_feed(
            published_from=published_from.isoformat(),
            published_to=published_to.isoformat(),
            limit=PAGE_SIZE,
            offset=page * PAGE_SIZE,
        )
    except ApiError as exc:
        st.error(str(exc))
        return

    previous_date = _parse_api_date(adjacent_dates.get("previous_date"))
    next_date = _parse_api_date(adjacent_dates.get("next_date"))
    previous_col, next_col = st.columns(2)
    with previous_col:
        if st.button(
            "← Предыдущая дата с новостями",
            disabled=previous_date is None,
            key="date-news-adjacent-previous",
            use_container_width=True,
        ):
            _select_date(previous_date)
    with next_col:
        if st.button(
            "Следующая дата с новостями →",
            disabled=next_date is None,
            key="date-news-adjacent-next",
            use_container_width=True,
        ):
            _select_date(next_date)

    items = result.get("items") or []
    total = int(result.get("total") or 0)
    total_clusters = int(result.get("total_clusters") or 0)
    if not items:
        st.info("За выбранную дату обработанных новостей не найдено.")
        return

    st.caption(f"Сюжетов: {total_clusters} · публикаций: {total}")
    render_search_result(
        result,
        key_prefix=f"date-feed-{selected_date.isoformat()}-{page}",
    )

    previous_col, page_col, next_col = st.columns([1, 2, 1])
    with previous_col:
        if st.button(
            "← Назад",
            disabled=page == 0,
            key="date-news-previous",
            use_container_width=True,
        ):
            st.session_state["date_news_page"] = page - 1
            st.rerun()
    with page_col:
        st.markdown(
            f"<div style='text-align:center'>Страница {page + 1}</div>",
            unsafe_allow_html=True,
        )
    with next_col:
        if st.button(
            "Далее →",
            disabled=(page + 1) * PAGE_SIZE >= total_clusters,
            key="date-news-next",
            use_container_width=True,
        ):
            st.session_state["date_news_page"] = page + 1
            st.rerun()


def _parse_api_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(MOSCOW_TIMEZONE).date()


def _select_date(selected_date: date | None) -> None:
    if selected_date is None:
        return
    st.session_state["date_news_selected_date"] = selected_date
    st.session_state["date_news_page"] = 0
    st.rerun()
