from __future__ import annotations

import pandas as pd
import streamlit as st

from api_client import ApiClient, ApiError, is_authentication_error
from formatting import format_amount, format_search_date


def render_transactions(client: ApiClient) -> None:
    st.header("Операции")
    reason_labels = {
        "": "Все операции",
        "news_add": "Публикация новости",
        "news_reprocess": "Повторная обработка новости",
        "news_search": "Поиск новостей",
        "credit_add": "Пополнение баланса",
        "credit_withdraw": "Списание средств",
    }
    reason = st.selectbox(
        "Тип операции",
        list(reason_labels),
        format_func=reason_labels.get,
    )
    try:
        is_admin = (st.session_state.get("me") or {}).get("role") == "admin"
        transactions = client.list_transactions(reason or None, admin=is_admin)
        if transactions:
            rows = [
                {
                    "Дата": format_search_date(item.get("timestamp")),
                    "Операция": reason_labels.get(
                        item.get("reason"),
                        item.get("reason"),
                    ),
                    "Комментарий": _transaction_object_label(item),
                    "Источник": item.get("reference_url") or "",
                    "Сумма": format_amount(item.get("amount", 0)),
                    **(
                        {
                            "Организация": item.get("organization_name") or "—",
                            "Пользователь": item.get("actor_login") or "—",
                        }
                        if is_admin
                        else {}
                    ),
                }
                for item in transactions
            ]
            st.dataframe(
                pd.DataFrame(rows),
                hide_index=True,
                width="stretch",
                column_config={
                    "Дата": st.column_config.TextColumn("Дата", width="small"),
                    "Операция": st.column_config.TextColumn(
                        "Операция",
                        width="medium",
                    ),
                    "Комментарий": st.column_config.TextColumn(
                        "Комментарий",
                        width="large",
                    ),
                    "Источник": st.column_config.LinkColumn(
                        "Источник",
                        display_text="Открыть",
                        width="small",
                    ),
                    "Сумма": st.column_config.TextColumn("Сумма", width="small"),
                },
            )
        else:
            st.info("Операций пока нет.")
    except ApiError as exc:
        if is_authentication_error(exc):
            raise
        st.error(str(exc))


def _transaction_object_label(item: dict) -> str:
    """Вернуть понятный комментарий к операции."""
    item_count = int(item.get("item_count") or 1)
    if item.get("batch_id") and item_count > 1:
        if item.get("reason") == "news_reprocess":
            return f"Пакетная повторная обработка: {item_count} новостей"
        return f"Пакетная публикация: {item_count} новостей"
    return item.get("reference_title") or "—"
