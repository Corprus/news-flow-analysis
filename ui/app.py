from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation

import pandas as pd
import streamlit as st

from api_client import ApiClient, ApiError

API_INTERNAL = os.getenv("API_BASE_URL", "http://nginx/api")

st.set_page_config(page_title="News Flow", layout="wide")


def get_client() -> ApiClient:
    if "client" not in st.session_state:
        st.session_state.client = ApiClient(API_INTERNAL)
    return st.session_state.client


client = get_client()


def refresh_account() -> None:
    st.session_state["me"] = client.get_me()
    st.session_state["balance"] = client.get_balance()


def parse_decimal(value: str) -> Decimal | None:
    try:
        amount = Decimal(value.strip())
    except (InvalidOperation, ValueError):
        return None
    return amount if amount > 0 else None


def render_login() -> None:
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
            client.set_token(client.login(login, password))
            refresh_account()
            st.rerun()
        if do_login:
            client.set_token(client.login(login, password))
            refresh_account()
            st.rerun()
    except ApiError as exc:
        st.error(str(exc))


def render_sidebar() -> str:
    me = st.session_state.get("me") or {}
    balance = st.session_state.get("balance") or {}
    with st.sidebar:
        st.subheader(me.get("login", "User"))
        st.caption(me.get("role", ""))
        st.metric("Credits", balance.get("balance", "0.00"))
        if st.button("Refresh account", use_container_width=True):
            refresh_account()
            st.rerun()
        if st.button("Logout", use_container_width=True):
            client.logout()
            st.session_state.clear()
            st.rerun()

        pages = ["Search", "News", "History", "Transactions"]
        if str(me.get("role", "")).lower() == "admin":
            pages.append("Admin")
        return st.radio("Navigation", pages, label_visibility="collapsed")


def render_news() -> None:
    st.header("Add News")
    with st.form("add_news_form"):
        title = st.text_input("Title")
        content = st.text_area("Content", height=260)
        col_url, col_lang = st.columns([3, 1])
        url = col_url.text_input("URL")
        language = col_lang.text_input("Language", placeholder="ru")
        summary = st.text_area("Summary", height=100)
        submitted = st.form_submit_button("Add and queue vectorization")

    if submitted:
        payload = {
            "title": title,
            "content": content,
            "summary": summary or None,
            "url": url or None,
            "canonical_url": url or None,
            "language": language or None,
        }
        try:
            result = client.add_news(payload)
            refresh_account()
            st.success(
                f"News queued: {result['article_id']} "
                f"(job {result['vectorization_job_id']})"
            )
        except ApiError as exc:
            st.error(str(exc))

    st.subheader("My Added News")
    try:
        history = client.list_news_history()
        if history:
            st.dataframe(pd.DataFrame(history), use_container_width=True, hide_index=True)
        else:
            st.info("No user-added news yet.")
    except ApiError as exc:
        st.error(str(exc))


def render_search() -> None:
    st.header("Search News")
    with st.form("search_form"):
        query_text = st.text_input("Semantic query")
        col_topk, col_lang, col_novelty = st.columns(3)
        top_k = col_topk.number_input("Top K", min_value=1, max_value=100, value=20)
        language = col_lang.text_input("Language filter", placeholder="ru")
        min_novelty = col_novelty.number_input(
            "Min novelty",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.05,
        )
        col_from, col_to = st.columns(2)
        published_from = col_from.text_input("Published from ISO")
        published_to = col_to.text_input("Published to ISO")
        submitted = st.form_submit_button("Queue search")

    if submitted:
        payload = {
            "query_text": query_text,
            "top_k": int(top_k),
            "language": language or None,
            "published_from": published_from or None,
            "published_to": published_to or None,
            "min_novelty_score": float(min_novelty) if min_novelty > 0 else None,
        }
        try:
            result = client.search_news(payload)
            refresh_account()
            st.success(
                f"Search queued: {result['query_id']} "
                f"(job {result['vectorization_job_id']})"
            )
        except ApiError as exc:
            st.error(str(exc))

    render_search_history()


def render_search_history() -> None:
    st.subheader("Search History")
    try:
        history = client.list_search_history()
        if history:
            st.dataframe(pd.DataFrame(history), use_container_width=True, hide_index=True)
        else:
            st.info("No searches yet.")
    except ApiError as exc:
        st.error(str(exc))


def render_history() -> None:
    left, right = st.columns(2)
    with left:
        st.subheader("News History")
        try:
            news = client.list_news_history()
            st.dataframe(pd.DataFrame(news), use_container_width=True, hide_index=True)
        except ApiError as exc:
            st.error(str(exc))
    with right:
        render_search_history()


def render_transactions() -> None:
    st.header("Transactions")
    reason = st.selectbox(
        "Reason",
        ["", "news_add", "news_search", "credit_add", "credit_withdraw"],
    )
    try:
        transactions = client.list_transactions(reason or None)
        if transactions:
            st.dataframe(pd.DataFrame(transactions), use_container_width=True, hide_index=True)
        else:
            st.info("No transactions.")
    except ApiError as exc:
        st.error(str(exc))


def render_admin() -> None:
    st.header("Admin")
    try:
        users = client.list_users()
    except ApiError as exc:
        st.error(str(exc))
        return

    st.dataframe(pd.DataFrame(users), use_container_width=True, hide_index=True)
    with st.form("credit_form"):
        user_id = st.text_input("User ID")
        amount_raw = st.text_input("Amount", value="10.00")
        submitted = st.form_submit_button("Add credits")
    if submitted:
        amount = parse_decimal(amount_raw)
        if amount is None:
            st.warning("Invalid amount")
            return
        try:
            client.add_credit(user_id, amount)
            st.success("Credits added")
        except ApiError as exc:
            st.error(str(exc))


if not client.token:
    render_login()
    st.stop()

if "me" not in st.session_state:
    try:
        refresh_account()
    except ApiError as exc:
        st.error(str(exc))
        st.stop()

page = render_sidebar()

if page == "Search":
    render_search()
elif page == "News":
    render_news()
elif page == "History":
    render_history()
elif page == "Transactions":
    render_transactions()
elif page == "Admin":
    render_admin()
