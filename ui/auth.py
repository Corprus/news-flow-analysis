from __future__ import annotations

from datetime import UTC, datetime, timedelta

import streamlit as st

from api_client import ApiClient, ApiError
from config import API_INTERNAL, AUTH_COOKIE_NAME, AUTH_COOKIE_TTL_DAYS


def get_client(cookie_manager: object) -> ApiClient:
    logout_pending = st.session_state.get("logout_pending", False)
    cookie_token = None if logout_pending else cookie_manager.get(AUTH_COOKIE_NAME)
    if "client" not in st.session_state:
        st.session_state.client = ApiClient(
            API_INTERNAL,
            token=cookie_token,
        )
    elif logout_pending:
        st.session_state.client.logout()
    elif not st.session_state.client.token and cookie_token:
        # The cookie component is asynchronous: immediately after F5 its first
        # render can return no cookies and trigger another run once they load.
        st.session_state.client.set_token(cookie_token)
    return st.session_state.client


def clear_pending_auth_cookie(cookie_manager: object) -> None:
    if not st.session_state.get("logout_pending"):
        return
    if cookie_manager.get(AUTH_COOKIE_NAME) is None:
        return
    try:
        cookie_manager.delete(AUTH_COOKIE_NAME, key="delete_auth_cookie")
    except KeyError:
        # CookieManager may receive its asynchronous cookie snapshot between
        # get() and delete(). The next rerun will retry while logout is pending.
        pass


def persist_token(client: ApiClient, cookie_manager: object, token: str) -> None:
    st.session_state.pop("logout_pending", None)
    client.set_token(token)
    cookie_manager.set(
        AUTH_COOKIE_NAME,
        token,
        expires_at=datetime.now(UTC) + timedelta(days=AUTH_COOKIE_TTL_DAYS),
        key="set_auth_cookie",
    )


def clear_authentication(client: ApiClient) -> None:
    client.logout()
    st.session_state["logout_pending"] = True
    for key in ("me", "balance", "active_page"):
        st.session_state.pop(key, None)


def refresh_account(client: ApiClient) -> None:
    st.session_state["me"] = client.get_me()
    st.session_state["balance"] = client.get_balance()


def render_login(client: ApiClient, cookie_manager: object) -> None:
    st.title("News Flow")
    st.caption("Semantic news search with credits, user-added news, and future event grouping.")
    with st.form("login_form"):
        login = st.text_input("Login")
        password = st.text_input("Password", type="password")
        col_login, col_signup = st.columns(2)
        do_login = col_login.form_submit_button("Sign in", use_container_width=True)
        do_signup = col_signup.form_submit_button("Create user", use_container_width=True)

    try:
        if do_signup:
            client.create_user(login, password)
            persist_token(client, cookie_manager, client.login(login, password))
            refresh_account(client)
        if do_login:
            persist_token(client, cookie_manager, client.login(login, password))
            refresh_account(client)
    except ApiError as exc:
        st.error(str(exc))
