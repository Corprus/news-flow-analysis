from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

import pandas as pd
import streamlit as st

from api_client import ApiClient, ApiError
from auth import refresh_account
from formatting import format_search_date

ROLE_LABELS = {
    "user": "Пользователь",
    "publisher": "Редактор",
    "admin": "Администратор",
}
AUDIT_ACTION_LABELS = {
    "user.create": "Создание пользователя",
    "user.update": "Изменение пользователя",
    "user.role.update": "Изменение роли",
    "user.delete": "Удаление пользователя",
    "organization.create": "Создание организации",
    "organization.update": "Изменение организации",
    "balance.adjust": "Корректировка баланса",
}
AUDIT_TARGET_LABELS = {
    "user": "Пользователь",
    "organization": "Организация",
}
AUDIT_DETAIL_LABELS = {
    "login": "Логин",
    "role": "Роль",
    "previous_role": "Предыдущая роль",
    "organization_id": "ID организации",
    "name": "Название",
    "previous_name": "Предыдущее название",
    "amount": "Сумма",
    "transaction_id": "ID операции",
}
ADMIN_FLASH_KEY = "admin_success_message"


def render_admin(client: ApiClient) -> None:
    st.header("Администрирование")
    success_message = st.session_state.pop(ADMIN_FLASH_KEY, None)
    if success_message:
        st.success(success_message)
    try:
        users = client.list_users()
        organizations = client.list_organizations()
    except ApiError as exc:
        st.error(str(exc))
        return

    organization_by_id = {item["id"]: item for item in organizations}
    user_by_id = {item["id"]: item for item in users}
    users_tab, organizations_tab, audit_tab = st.tabs(
        ["Пользователи", "Организации", "Журнал действий"]
    )

    with users_tab:
        _render_users(client, users, organizations, organization_by_id)
    with organizations_tab:
        _render_organizations(client, organizations)
    with audit_tab:
        _render_audit(client, user_by_id)


def _render_users(
    client: ApiClient,
    users: list[dict],
    organizations: list[dict],
    organization_by_id: dict[str, dict],
) -> None:
    filter_col, role_col = st.columns([2, 1])
    with filter_col:
        query = st.text_input("Поиск по логину", key="admin-user-query").strip().lower()
    with role_col:
        role_filter = st.selectbox(
            "Роль",
            ["", *ROLE_LABELS],
            format_func=lambda value: "Все роли" if not value else ROLE_LABELS[value],
            key="admin-user-role-filter",
        )

    filtered_users = [
        user
        for user in users
        if (not query or query in user["login"].lower())
        and (not role_filter or user["role"] == role_filter)
    ]
    rows = [
        {
            "ID": user["id"],
            "Логин": user["login"],
            "Роль": ROLE_LABELS.get(user["role"], user["role"]),
            "Организация": organization_by_id.get(user["organization_id"], {}).get(
                "name", user["organization_id"]
            ),
            "Удалить": False,
        }
        for user in filtered_users
    ]
    organization_names = [item["name"] for item in organizations]
    edited = st.data_editor(
        pd.DataFrame(rows),
        hide_index=True,
        width="stretch",
        disabled=["ID"],
        column_order=["Логин", "Роль", "Организация", "Удалить"],
        column_config={
            "Логин": st.column_config.TextColumn(
                "Логин",
                required=True,
                width="medium",
            ),
            "Роль": st.column_config.SelectboxColumn(
                "Роль",
                options=list(ROLE_LABELS.values()),
                required=True,
                width="medium",
            ),
            "Организация": st.column_config.SelectboxColumn(
                "Организация",
                options=organization_names,
                required=True,
                width="large",
            ),
            "Удалить": st.column_config.CheckboxColumn(
                "Удалить",
                help="Пользователь будет удалён после сохранения изменений.",
                width="small",
            ),
        },
        key="admin-users-editor",
    )
    if st.button("Сохранить изменения пользователей", type="primary"):
        _save_user_changes(client, users, organizations, edited)

    st.subheader("Создать пользователя")
    if not organizations:
        st.info("Сначала создайте организацию.")
    else:
        organization_options = [item["id"] for item in organizations]
        with st.form("admin-create-user"):
            login_col, password_col = st.columns(2)
            login = login_col.text_input("Логин")
            password = password_col.text_input("Пароль", type="password")
            organization_id = st.selectbox(
                "Организация",
                organization_options,
                format_func=lambda value: organization_by_id[value]["name"],
            )
            role = st.selectbox(
                "Роль пользователя",
                list(ROLE_LABELS),
                format_func=ROLE_LABELS.get,
            )
            submitted = st.form_submit_button("Создать")
        if submitted:
            if len(login.strip()) < 3 or len(password) < 8:
                st.warning("Логин должен содержать от 3 символов, пароль — от 8.")
            else:
                try:
                    client.create_admin_user(
                        login.strip(),
                        password,
                        role,
                        organization_id,
                    )
                    _rerun_with_success("Пользователь создан.")
                except ApiError as exc:
                    st.error(str(exc))


def _save_user_changes(
    client: ApiClient,
    users: list[dict],
    organizations: list[dict],
    edited: pd.DataFrame,
) -> None:
    original_by_id = {item["id"]: item for item in users}
    organization_id_by_name = {item["name"]: item["id"] for item in organizations}
    role_by_label = {label: role for role, label in ROLE_LABELS.items()}
    current_user_id = (st.session_state.get("me") or {}).get("id")
    logins: set[str] = set()
    updates: list[tuple[str, str, str, str]] = []
    deletions: list[str] = []

    for row in edited.to_dict("records"):
        user_id = str(row["ID"])
        login = str(row["Логин"]).strip()
        role = role_by_label.get(str(row["Роль"]))
        organization_id = organization_id_by_name.get(str(row["Организация"]))
        delete = bool(row["Удалить"])
        if len(login) < 3:
            st.warning("Логин должен содержать не менее трёх символов.")
            return
        normalized_login = login.casefold()
        if normalized_login in logins:
            st.warning("Логины пользователей не должны повторяться.")
            return
        logins.add(normalized_login)
        if role is None or organization_id is None:
            st.warning("Выберите корректную роль и организацию.")
            return
        if user_id == current_user_id and delete:
            st.warning("Нельзя удалить собственную учётную запись.")
            return
        if user_id == current_user_id and role != "admin":
            st.warning("Нельзя снять роль администратора у самого себя.")
            return
        if delete:
            deletions.append(user_id)
        else:
            updates.append((user_id, login, role, organization_id))

    changed_count = 0
    try:
        for user_id, login, role, organization_id in updates:
            original = original_by_id[user_id]
            if (
                login != original["login"]
                or role != original["role"]
                or organization_id != original["organization_id"]
            ):
                client.update_user(
                    user_id,
                    login=login,
                    role=role,
                    organization_id=organization_id,
                )
                changed_count += 1
        for user_id in deletions:
            client.delete_user(user_id)
            changed_count += 1
    except ApiError as exc:
        st.error(str(exc))
        return

    if changed_count == 0:
        st.info("Изменений нет.")
        return
    refresh_account(client)
    _rerun_with_success("Изменения пользователей сохранены.")


def _render_organizations(client: ApiClient, organizations: list[dict]) -> None:
    rows = [
        {
            "ID": item["id"],
            "Название": item["name"],
            "Пользователей": item["user_count"],
            "Баланс": int(Decimal(str(item["balance"]))),
            "Создана": format_search_date(item["created_at"]),
        }
        for item in organizations
    ]
    edited = st.data_editor(
        pd.DataFrame(rows),
        hide_index=True,
        width="stretch",
        disabled=["ID", "Пользователей", "Создана"],
        column_order=["Название", "Пользователей", "Баланс", "Создана", "ID"],
        column_config={
            "Название": st.column_config.TextColumn(
                "Название",
                required=True,
                width="large",
            ),
            "Пользователей": st.column_config.NumberColumn(
                "Пользователей",
                width="small",
            ),
            "Баланс": st.column_config.NumberColumn(
                "Баланс",
                min_value=0,
                step=1,
                format="%d",
                required=True,
                width="small",
            ),
            "Создана": st.column_config.TextColumn("Создана", width="medium"),
            "ID": st.column_config.TextColumn("ID", width="large"),
        },
        key="admin-organizations-editor",
    )
    if st.button("Сохранить изменения", type="primary"):
        _save_organization_changes(client, organizations, edited)

    st.subheader("Создать организацию")
    with st.form("admin-create-organization"):
        name = st.text_input("Название")
        create = st.form_submit_button("Создать")
    if create:
        if len(name.strip()) < 2:
            st.warning("Укажите название организации.")
        else:
            try:
                client.create_organization(name.strip())
                _rerun_with_success("Организация создана.")
            except ApiError as exc:
                st.error(str(exc))


def _save_organization_changes(
    client: ApiClient,
    organizations: list[dict],
    edited: pd.DataFrame,
) -> None:
    original_by_id = {item["id"]: item for item in organizations}
    changes: list[tuple[str, str, Decimal]] = []
    names: set[str] = set()
    for row in edited.to_dict("records"):
        organization_id = str(row["ID"])
        name = str(row["Название"]).strip()
        if len(name) < 2:
            st.warning("Название организации должно содержать не менее двух символов.")
            return
        normalized_name = name.casefold()
        if normalized_name in names:
            st.warning("Названия организаций не должны повторяться.")
            return
        names.add(normalized_name)
        try:
            balance = Decimal(str(row["Баланс"]))
        except (InvalidOperation, ValueError):
            st.warning("Баланс должен быть целым неотрицательным числом.")
            return
        if balance < 0 or balance != balance.to_integral_value():
            st.warning("Баланс должен быть целым неотрицательным числом.")
            return
        changes.append((organization_id, name, balance))

    changed_count = 0
    try:
        for organization_id, name, balance in changes:
            original = original_by_id[organization_id]
            if name != original["name"]:
                client.update_organization(organization_id, name)
                changed_count += 1
            current_balance = Decimal(str(original["balance"]))
            balance_delta = balance - current_balance
            if balance_delta:
                client.adjust_credit(organization_id, balance_delta)
                changed_count += 1
    except ApiError as exc:
        st.error(str(exc))
        return

    if changed_count == 0:
        st.info("Изменений нет.")
        return
    refresh_account(client)
    _rerun_with_success("Изменения организаций сохранены.")


def _render_audit(client: ApiClient, user_by_id: dict[str, dict]) -> None:
    action = st.selectbox(
        "Тип действия",
        ["", *AUDIT_ACTION_LABELS],
        format_func=lambda value: (
            "Все действия" if not value else AUDIT_ACTION_LABELS[value]
        ),
        key="admin-audit-action",
    )
    try:
        entries = client.list_admin_audit(action or None)
    except ApiError as exc:
        st.error(str(exc))
        return
    rows = [
        {
            "Дата": format_search_date(entry["created_at"]),
            "Администратор": user_by_id.get(entry.get("actor_user_id"), {}).get(
                "login", entry.get("actor_user_id") or "Удалённый пользователь"
            ),
            "Действие": AUDIT_ACTION_LABELS.get(entry["action"], entry["action"]),
            "Объект": (
                f"{AUDIT_TARGET_LABELS.get(entry['target_type'], entry['target_type'])}"
                f" · {entry.get('target_id') or '—'}"
            ),
            "Детали": _format_audit_details(entry.get("details", {})),
        }
        for entry in entries
    ]
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        st.info("Записей пока нет.")


def _format_audit_details(details: dict) -> str:
    localized = {}
    for key, value in details.items():
        label = AUDIT_DETAIL_LABELS.get(key, key)
        if key in {"role", "previous_role"}:
            value = ROLE_LABELS.get(value, value)
        localized[label] = value
    return json.dumps(localized, ensure_ascii=False)


def _rerun_with_success(message: str) -> None:
    st.session_state[ADMIN_FLASH_KEY] = message
    st.rerun()
