from __future__ import annotations

import html
import os
from datetime import UTC, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation

import extra_streamlit_components as stx
import pandas as pd
import streamlit as st

from api_client import ApiClient, ApiError

API_INTERNAL = os.getenv("API_BASE_URL", "http://nginx/api")
AUTH_COOKIE_NAME = "news_flow_access_token"
AUTH_COOKIE_TTL_DAYS = int(os.getenv("AUTH_COOKIE_TTL_DAYS", "1"))
MOSCOW_TIMEZONE = timezone(timedelta(hours=3))
PAGE_LABELS = {
    "Search": "Поиск",
    "News": "Новости",
    "History": "История",
    "Transactions": "Операции",
    "Admin": "Администрирование",
}
ROLE_LABELS = {
    "user": "Пользователь",
    "publisher": "Публикатор",
    "admin": "Администратор",
}

st.set_page_config(page_title="News Flow", layout="wide")
st.markdown(
    """
    <style>
    div[class*="st-key-cluster-expander-"] details > summary p,
    div[class*="st-key-search-expander-"] details > summary p {
        display: flex;
        justify-content: space-between;
        align-items: center;
        width: 100%;
        gap: 1rem;
    }
    div[class*="st-key-cluster-expander-"] details > summary p strong,
    div[class*="st-key-search-expander-"] details > summary p strong {
        font-weight: 600;
    }
    div[class*="st-key-cluster-expander-"] details > summary p,
    div[class*="st-key-search-expander-"] details > summary p {
        color: #8b949e;
        font-size: 0.9em;
    }
    div[class*="st-key-cluster-expander-"] details > summary p strong,
    div[class*="st-key-search-expander-"] details > summary p strong {
        color: #f0f2f6;
        font-size: 1.1em;
    }
    div[class*="st-key-cluster-expander-"] details > summary p a,
    div[class*="st-key-search-expander-"] details > summary p a {
        color: inherit;
        text-decoration: none;
    }
    div[class*="st-key-read-more-"] button {
        color: #58a6ff;
        padding: 0;
        min-height: auto;
        border: 0;
        background: transparent;
    }
    div[class*="st-key-read-more-"] button:hover {
        color: #79c0ff;
        background: transparent;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
cookie_manager = stx.CookieManager(key="auth_cookie_manager")


def get_client() -> ApiClient:
    cookie_token = cookie_manager.get(AUTH_COOKIE_NAME)
    if "client" not in st.session_state:
        st.session_state.client = ApiClient(
            API_INTERNAL,
            token=cookie_token,
        )
    elif not st.session_state.client.token and cookie_token:
        # The cookie component is asynchronous: immediately after F5 its first
        # render can return no cookies and trigger another run once they load.
        st.session_state.client.set_token(cookie_token)
    return st.session_state.client


client = get_client()


def persist_token(token: str) -> None:
    client.set_token(token)
    cookie_manager.set(
        AUTH_COOKIE_NAME,
        token,
        expires_at=datetime.now(UTC) + timedelta(days=AUTH_COOKIE_TTL_DAYS),
        key="set_auth_cookie",
    )


def clear_authentication() -> None:
    client.logout()
    cookie_manager.delete(AUTH_COOKIE_NAME, key="delete_auth_cookie")
    st.session_state.clear()


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
            persist_token(client.login(login, password))
            refresh_account()
        if do_login:
            persist_token(client.login(login, password))
            refresh_account()
    except ApiError as exc:
        st.error(str(exc))


def render_sidebar() -> str:
    me = st.session_state.get("me") or {}
    balance = st.session_state.get("balance") or {}
    with st.sidebar:
        st.subheader(me.get("login", "Пользователь"))
        role = str(me.get("role", "")).lower()
        st.caption(ROLE_LABELS.get(role, role))
        st.metric("Баланс", balance.get("balance", "0.00"))
        if st.button("Обновить баланс", use_container_width=True):
            refresh_account()
            st.rerun()
        if st.button("Выйти", use_container_width=True):
            clear_authentication()
            st.rerun()

        pages = ["Search"]
        if role in {"publisher", "admin"}:
            pages.append("News")
        pages.extend(["History", "Transactions"])
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

        return active_page


def render_news() -> None:
    st.header("Добавление новостей")
    manual_tab, file_tab = st.tabs(["Вручную", "Из файла"])
    with manual_tab:
        render_manual_news_form()
    with file_tab:
        render_news_file_import()

    st.subheader("Мои добавленные новости")
    try:
        history = client.list_news_history()
        if history:
            st.dataframe(pd.DataFrame(history), use_container_width=True, hide_index=True)
            drafts = [item for item in history if item.get("visibility") == "draft"]
            if drafts:
                draft_by_label = {
                    f"{item['title']} · {item['article_id']}": item["article_id"]
                    for item in drafts
                }
                selected_labels = st.multiselect(
                    "Drafts to publish",
                    list(draft_by_label),
                )
                if st.button(
                    "Publish selected",
                    disabled=not selected_labels,
                    use_container_width=True,
                ):
                    try:
                        result = client.publish_news_batch(
                            [draft_by_label[label] for label in selected_labels]
                        )
                        refresh_account()
                        st.success(
                            f"Published {result['published_count']} articles "
                            f"(job {result['job_id']})"
                        )
                        st.rerun()
                    except ApiError as exc:
                        st.error(str(exc))
            else:
                st.caption("No drafts available for publication.")
        else:
            st.info("No user-added news yet.")
    except ApiError as exc:
        st.error(str(exc))


def render_manual_news_form() -> None:
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
        publish_immediately = st.checkbox("Publish immediately")
        submitted = st.form_submit_button(
            "Save and publish" if publish_immediately else "Save draft"
        )

    if submitted:
        payload = {
            "title": title,
            "content": content,
            "summary": summary or None,
            "url": url or None,
            "canonical_url": url or None,
            "language": language or None,
            "published_at": published_at,
            "publish_immediately": publish_immediately,
        }
        try:
            result = client.add_news(payload)
            if result.get("job_id"):
                refresh_account()
                st.success(
                    f"Published: {result['article_id']} (job {result['job_id']})"
                )
            else:
                st.success(f"Draft saved: {result['article_id']}")
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
        label = st.selectbox("Format", list(format_by_label))
        selected_format = format_by_label[label]
        extensions = [
            extension.lstrip(".")
            for extension in selected_format.get("file_extensions", [])
        ]
        uploaded_file = st.file_uploader("News file", type=extensions or None)
        publish_immediately = st.checkbox(
            "Publish immediately",
            key="import-publish-immediately",
        )
        submitted = st.form_submit_button(
            "Import and publish" if publish_immediately else "Import drafts"
        )

    if submitted:
        if uploaded_file is None:
            st.warning("Select a file to import.")
            return
        try:
            result = client.import_news(
                selected_format["id"],
                uploaded_file.name,
                uploaded_file.getvalue(),
                publish_immediately=publish_immediately,
            )
            message = (
                "Import completed: "
                f"{result['created_count']} created, "
                f"{result['duplicate_count']} duplicates linked"
            )
            if result.get("published_count"):
                refresh_account()
                message += (
                    f", {result['published_count']} published "
                    f"(job {result['job_id']})"
                )
            st.success(message + ".")
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
            col_topk, col_lang, col_novelty = st.columns(3)
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
            for item in visible_items:
                render_search_article(item)
            hidden_count = len(items) - len(visible_items)
            if hidden_count > 0 and not show_all:
                st.caption(f"Скрыто публикаций: {hidden_count}")


def render_search_article(item: dict) -> None:
    title = html.escape(str(item.get("title") or "Без названия"))
    novelty_label = item.get("novelty_label")
    is_significant = novelty_label == "significant"
    details = [format_search_date(item.get("published_at"))]
    if item.get("score") is not None:
        details.append(f"релевантность {float(item['score']):.3f}")
    if item.get("p_significant") is not None:
        details.append(f"значимость {float(item['p_significant']):.3f}")
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
            state_key = f"article-expanded-{article_id}"
            is_expanded = st.session_state.get(state_key, False)
            visible_text = article_text if is_expanded else preview + "…"
            st.markdown(
                f"<div style='color:{text_color}'>{html.escape(visible_text)}</div>",
                unsafe_allow_html=True,
            )
            if st.button(
                "Свернуть ↑" if is_expanded else "Читать далее →",
                key=f"read-more-{article_id}",
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


def format_search_result_summary(result: dict) -> str:
    clusters = result.get("clusters") or []
    if not clusters:
        return ""
    publication_count = sum(
        int(cluster.get("article_count", len(cluster.get("items", []))))
        for cluster in clusters
    )
    return (
        f'[🗂](# "Количество сюжетов") {len(clusters)}'
        f' · [📰](# "Количество публикаций") {publication_count}'
    )


def format_search_date(value: str | None, *, date_only: bool = False) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.strftime("%d.%m.%Y" if date_only else "%d.%m.%Y %H:%M")
    except (TypeError, ValueError):
        return str(value)


def escape_markdown(value: str) -> str:
    for character in ("\\", "*", "_", "`", "[", "]"):
        value = value.replace(character, f"\\{character}")
    return value


def render_history() -> None:
    left, right = st.columns(2)
    with left:
        st.subheader("История новостей")
        try:
            news = client.list_news_history()
            st.dataframe(pd.DataFrame(news), use_container_width=True, hide_index=True)
        except ApiError as exc:
            st.error(str(exc))
    with right:
        render_search_history()


def render_transactions() -> None:
    st.header("Операции")
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
    render_login()
    if not client.token:
        st.stop()

if "me" not in st.session_state:
    try:
        refresh_account()
    except ApiError as exc:
        if exc.status_code == 401:
            clear_authentication()
            st.rerun()
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
