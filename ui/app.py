from __future__ import annotations

import os
from datetime import UTC, datetime
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

        role = str(me.get("role", "")).lower()
        pages = ["Search"]
        if role in {"publisher", "admin"}:
            pages.append("News")
        pages.extend(["History", "Transactions"])
        if role == "admin":
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
        published_at = st.text_input(
            "Published at (ISO 8601)",
            value=datetime.now(UTC).isoformat(),
        )
        summary = st.text_area("Summary", height=100)
        submitted = st.form_submit_button("Save draft")

    if submitted:
        payload = {
            "title": title,
            "content": content,
            "summary": summary or None,
            "url": url or None,
            "canonical_url": url or None,
            "language": language or None,
            "published_at": published_at,
        }
        try:
            result = client.add_news(payload)
            st.success(f"Draft saved: {result['article_id']}")
        except ApiError as exc:
            st.error(str(exc))

    st.subheader("My Added News")
    try:
        history = client.list_news_history()
        if history:
            st.dataframe(pd.DataFrame(history), use_container_width=True, hide_index=True)
            drafts = [item for item in history if item.get("visibility") == "draft"]
            for item in drafts:
                article_id = item["article_id"]
                if st.button(
                    f"Publish: {item['title']}",
                    key=f"publish-{article_id}",
                    use_container_width=True,
                ):
                    try:
                        result = client.publish_news(article_id)
                        refresh_account()
                        st.success(
                            f"Published: {article_id} (job {result['job_id']})"
                        )
                        st.rerun()
                    except ApiError as exc:
                        st.error(str(exc))
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
                f"Search queued: {result['query_id']} (job {result['job_id']})"
            )
        except ApiError as exc:
            st.error(str(exc))

    render_search_history()


def render_search_history() -> None:
    st.subheader("Search History")
    try:
        history = client.list_search_history()
        if not history:
            st.info("No searches yet.")
            return
        for search in history:
            status = search.get("status", "unknown")
            query_text = search.get("query_text", "")
            created_at = format_search_date(search.get("created_at"))
            with st.expander(f"{query_text} · {status} · {created_at}"):
                if status == "done":
                    render_search_result(
                        search.get("result") or {},
                        key_prefix=str(search.get("query_id") or created_at),
                    )
                elif status == "failed":
                    st.error(search.get("error") or "Search failed.")
                else:
                    st.info(f"Search status: {status}")
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
        st.info("No matching news found.")
        return

    st.caption(f"{len(clusters)} matching story clusters")
    for cluster_index, cluster in enumerate(clusters):
        title = cluster.get("representative_title") or "Untitled story"
        article_count = cluster.get("article_count", len(cluster.get("items", [])))
        significant_count = cluster.get("significant_count", 0)
        label = f"{title} · {article_count} articles"
        if significant_count:
            label += f" · {significant_count} significant"
        with st.expander(label, expanded=cluster_index == 0):
            items = cluster.get("items", [])
            show_all = st.checkbox(
                "Show duplicates and all matches",
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
            for item in visible_items:
                render_search_article(item)
            hidden_count = len(items) - len(visible_items)
            if hidden_count > 0 and not show_all:
                st.caption(f"{hidden_count} more or duplicate articles hidden")


def render_search_article(item: dict) -> None:
    title = escape_markdown(str(item.get("title") or "Untitled article"))
    novelty_label = item.get("novelty_label")
    if novelty_label == "significant":
        st.markdown(f"**{title}** · `significant update`")
    else:
        suffix = f" · `{novelty_label}`" if novelty_label else ""
        st.markdown(f"{title}{suffix}")

    details = [format_search_date(item.get("published_at"))]
    if item.get("score") is not None:
        details.append(f"relevance {float(item['score']):.3f}")
    if item.get("p_significant") is not None:
        details.append(f"significance {float(item['p_significant']):.3f}")
    st.caption(" · ".join(detail for detail in details if detail))
    url = item.get("url")
    if isinstance(url, str) and url.startswith(("http://", "https://")):
        st.markdown(f"[Open source]({url})")


def format_search_date(value: str | None) -> str:
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime(
            "%Y-%m-%d %H:%M"
        )
    except (TypeError, ValueError):
        return str(value)


def escape_markdown(value: str) -> str:
    for character in ("\\", "*", "_", "`", "[", "]"):
        value = value.replace(character, f"\\{character}")
    return value


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
