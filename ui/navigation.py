from __future__ import annotations

import html
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import streamlit as st

from api_client import ApiClient, ApiError
from auth import clear_authentication, refresh_account
from config import MIN_NEWS_DATE, MOSCOW_TIMEZONE, PAGE_LABELS, ROLE_LABELS

DATE_NEWS_SELECTED_KEY = "date_news_selected_date"
SIDEBAR_NEWS_DATE_KEY = "sidebar_news_date"


def render_sidebar(client: ApiClient) -> str:
    me = st.session_state.get("me") or {}
    balance = st.session_state.get("balance") or {}
    with st.sidebar:
        role = str(me.get("role", "")).lower()
        organization_name = str(me.get("organization_name", "")).strip()
        organization_html = (
            "<div class='sidebar-organization'>"
            f"{html.escape(organization_name)}"
            "</div>"
            if organization_name
            else ""
        )
        st.markdown(
            (
                "<div class='sidebar-profile'>"
                f"{html.escape(str(me.get('login', 'Пользователь')))}"
                f" <span>· {html.escape(ROLE_LABELS.get(role, role))}</span>"
                f"{organization_html}"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        if role in {"publisher", "admin"}:
            try:
                balance_value = Decimal(str(balance.get("balance", "0")))
                balance_text = (
                    str(int(balance_value))
                    if balance_value == balance_value.to_integral_value()
                    else format(balance_value.normalize(), "f")
                )
            except (InvalidOperation, ValueError):
                balance_text = str(balance.get("balance", "0"))
            balance_col, refresh_col = st.columns([4, 2], vertical_alignment="center")
            with balance_col:
                st.markdown(
                    (
                        "<div class='organization-balance'>"
                        "Баланс организации · "
                        f"<strong>{balance_text}</strong>"
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )
            with refresh_col:
                if st.button(
                    "↻",
                    key="refresh-balance",
                    help="Обновить баланс",
                    type="tertiary",
                ):
                    refresh_account(client)
                    st.rerun()
        pages = ["Search", "DateNews"]
        if role in {"publisher", "admin"}:
            pages.extend(["News", "Transactions"])
        if role == "admin":
            pages.append("Admin")

        active_page = st.session_state.get("active_page", pages[0])
        if active_page not in pages:
            active_page = pages[0]
            st.session_state["active_page"] = active_page

        st.markdown("### Меню")
        for page_name in pages:
            if st.button(
                PAGE_LABELS[page_name],
                key=f"nav_{page_name.lower()}",
                type="primary" if page_name == active_page else "secondary",
                use_container_width=True,
            ):
                st.session_state["active_page"] = page_name
                st.rerun()

        st.markdown("### Новости по дате")
        selected_date = _get_selected_news_date(client)
        if st.session_state.get(SIDEBAR_NEWS_DATE_KEY) != selected_date:
            st.session_state[SIDEBAR_NEWS_DATE_KEY] = selected_date
        st.date_input(
            "Выберите день",
            value=selected_date,
            min_value=MIN_NEWS_DATE,
            format="DD.MM.YYYY",
            key=SIDEBAR_NEWS_DATE_KEY,
            on_change=_sync_sidebar_date,
        )
        if st.button(
            "Показать новости",
            key="show-news-by-date",
            disabled=st.session_state.get(SIDEBAR_NEWS_DATE_KEY) is None,
            use_container_width=True,
        ):
            st.session_state["date_news_page"] = 0
            st.session_state["active_page"] = "DateNews"
            st.rerun()

        if st.button(
            "Выйти",
            key="sidebar-logout",
            help="Завершить сеанс",
            type="tertiary",
        ):
            clear_authentication(client)
            st.rerun()

        return active_page


def _sync_sidebar_date() -> None:
    st.session_state[DATE_NEWS_SELECTED_KEY] = st.session_state.get(
        SIDEBAR_NEWS_DATE_KEY
    )
    st.session_state["date_news_page"] = 0


def _get_selected_news_date(client: ApiClient) -> date | None:
    selected_date = st.session_state.get(DATE_NEWS_SELECTED_KEY)
    if isinstance(selected_date, date):
        return selected_date

    try:
        latest = client.get_latest_news_date()
    except ApiError:
        return None

    latest_date = _parse_api_date(latest.get("latest_date"))
    if latest_date is not None:
        st.session_state[DATE_NEWS_SELECTED_KEY] = latest_date
        st.session_state["date_news_page"] = 0
    return latest_date


def _parse_api_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(MOSCOW_TIMEZONE).date()
