from __future__ import annotations

import pandas as pd
import streamlit as st

from api_client import ApiClient, ApiError
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
        transactions = client.list_transactions(reason or None)
        if transactions:
            rows = [
                {
                    "Дата": format_search_date(item.get("timestamp")),
                    "Операция": reason_labels.get(
                        item.get("reason"),
                        item.get("reason"),
                    ),
                    "Новость": (
                        (
                            f"Пакетная повторная обработка: "
                            f"{item.get('item_count')} новостей"
                            if item.get("reason") == "news_reprocess"
                            else f"Пакетная публикация: {item.get('item_count')} новостей"
                        )
                        if item.get("batch_id") and item.get("item_count", 1) > 1
                        else item.get("reference_title") or "—"
                    ),
                    "Источник": item.get("reference_url") or "",
                    "Сумма": format_amount(item.get("amount", 0)),
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
                    "Новость": st.column_config.TextColumn(
                        "Новость",
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
        st.error(str(exc))
