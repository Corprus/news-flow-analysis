from __future__ import annotations

import html
from decimal import Decimal, InvalidOperation

import streamlit as st

from api_client import ApiClient
from auth import clear_authentication, refresh_account
from config import PAGE_LABELS, ROLE_LABELS


def render_sidebar(client: ApiClient) -> str:
    me = st.session_state.get("me") or {}
    balance = st.session_state.get("balance") or {}
    with st.sidebar:
        role = str(me.get("role", "")).lower()
        st.markdown(
            (
                "<div class='sidebar-profile'>"
                f"{html.escape(str(me.get('login', 'Пользователь')))}"
                f" <span>· {html.escape(ROLE_LABELS.get(role, role))}</span>"
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
        pages = ["Search"]
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

        if st.button(
            "Выйти",
            key="sidebar-logout",
            help="Завершить сеанс",
            type="tertiary",
        ):
            clear_authentication(client)
            st.rerun()

        return active_page
