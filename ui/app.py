from __future__ import annotations

import extra_streamlit_components as stx
import streamlit as st

from api_client import ApiError
from auth import (
    clear_authentication,
    clear_pending_auth_cookie,
    get_client,
    refresh_account,
    render_login,
)
from navigation import render_sidebar
from styles import apply_styles
from views.admin import render_admin
from views.date_news import render_date_news
from views.news import render_news
from views.search import render_search
from views.transactions import render_transactions

st.set_page_config(page_title="Semantic News Novelty", layout="wide")
apply_styles()

cookie_manager = stx.CookieManager(key="auth_cookie_manager")
client = get_client(cookie_manager)
clear_pending_auth_cookie(cookie_manager)

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
    render_search(client)
elif page == "DateNews":
    render_date_news(client)
elif page == "News":
    render_news(client)
elif page == "Transactions":
    render_transactions(client)
elif page == "Admin":
    render_admin(client)
