from uuid import UUID

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from accounting.models import Account
from users.deps import authenticate
from users.exceptions import LastAdministratorError, UserAlreadyExistsError
from users.models import AdminAuditLog, Organization, User, UserRole
from users.passwords import PasswordHasher
from users.routes import CreateUserRequest
from users.service import AdminAuditService, OrganizationService, UserService
from users.tokens import AccessTokenHandler


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Organization.__table__.create(engine)
    User.__table__.create(engine)
    Account.__table__.create(engine)
    AdminAuditLog.__table__.create(engine)
    with Session(engine) as session:
        yield session


def test_public_user_request_rejects_role_escalation() -> None:
    with pytest.raises(ValidationError):
        CreateUserRequest(
            login="attacker",
            password="long-enough-password",
            role="admin",
        )


def test_admin_services_manage_organization_role_and_audit(session: Session) -> None:
    organizations = OrganizationService(session)
    users = UserService(session, PasswordHasher("test-secret"))
    audit = AdminAuditService(session)

    organization = organizations.create("Research")
    admin = users.create_user(
        "admin",
        "admin-password",
        UserRole.ADMIN,
        organization_id=UUID(organization.id),
    )
    member = users.create_user(
        "member",
        "member-password",
        organization_id=UUID(organization.id),
    )
    users.update_role(UUID(member.id), UserRole.PUBLISHER)
    organizations.update_name(UUID(organization.id), "Research Lab")
    audit.record(
        actor_user_id=UUID(admin.id),
        action="user.role.update",
        target_type="user",
        target_id=member.id,
        details={"previous_role": "user", "role": "publisher"},
    )

    summaries = organizations.list_with_summary()
    entries = audit.list_entries()

    assert member.role == UserRole.PUBLISHER.value
    assert summaries[0][0].name == "Research Lab"
    assert summaries[0][1] == 2
    assert entries[0].action == "user.role.update"
    assert entries[0].details["role"] == "publisher"


def test_organization_names_are_unique_in_admin_service(session: Session) -> None:
    organizations = OrganizationService(session)
    organizations.create("Research")

    with pytest.raises(ValueError, match="already exists"):
        organizations.create("Research")


def test_authentication_uses_current_database_role(session: Session) -> None:
    organizations = OrganizationService(session)
    users = UserService(session, PasswordHasher("test-secret"))
    organization = organizations.create("Research")
    user = users.create_user(
        "admin",
        "admin-password",
        UserRole.ADMIN,
        organization_id=UUID(organization.id),
    )
    users.create_user(
        "second-admin",
        "second-admin-password",
        UserRole.ADMIN,
        organization_id=UUID(organization.id),
    )
    tokens = AccessTokenHandler("token-secret", ttl_minutes=5)
    token = tokens.create_access_token(
        UUID(user.id),
        UUID(organization.id),
        UserRole.ADMIN,
    )
    users.update_role(UUID(user.id), UserRole.USER)

    current_user = authenticate(tokens, session, f"Bearer {token}")

    assert current_user.role == UserRole.USER


def test_admin_can_update_user_login_role_and_organization(session: Session) -> None:
    organizations = OrganizationService(session)
    users = UserService(session, PasswordHasher("test-secret"))
    first = organizations.create("First")
    second = organizations.create("Second")
    user = users.create_user(
        "member",
        "member-password",
        organization_id=UUID(first.id),
    )

    updated = users.update_user(
        UUID(user.id),
        login="editor",
        role=UserRole.PUBLISHER,
        organization_id=UUID(second.id),
    )

    assert updated.login == "editor"
    assert updated.role == UserRole.PUBLISHER.value
    assert updated.organization_id == second.id


def test_admin_cannot_update_user_to_existing_login(session: Session) -> None:
    organizations = OrganizationService(session)
    users = UserService(session, PasswordHasher("test-secret"))
    organization = organizations.create("Research")
    first = users.create_user(
        "first",
        "first-password",
        organization_id=UUID(organization.id),
    )
    users.create_user(
        "second",
        "second-password",
        organization_id=UUID(organization.id),
    )

    with pytest.raises(UserAlreadyExistsError):
        users.update_user(
            UUID(first.id),
            login="second",
            role=UserRole.USER,
            organization_id=UUID(organization.id),
        )


def test_last_administrator_cannot_be_demoted(session: Session) -> None:
    organizations = OrganizationService(session)
    users = UserService(session, PasswordHasher("test-secret"))
    organization = organizations.create("Research")
    admin = users.create_user(
        "admin",
        "admin-password",
        UserRole.ADMIN,
        organization_id=UUID(organization.id),
    )

    with pytest.raises(LastAdministratorError):
        users.update_user(
            UUID(admin.id),
            login=admin.login,
            role=UserRole.PUBLISHER,
            organization_id=UUID(organization.id),
        )

    assert admin.role == UserRole.ADMIN.value


def test_last_administrator_cannot_be_deleted(session: Session) -> None:
    organizations = OrganizationService(session)
    users = UserService(session, PasswordHasher("test-secret"))
    organization = organizations.create("Research")
    admin = users.create_user(
        "admin",
        "admin-password",
        UserRole.ADMIN,
        organization_id=UUID(organization.id),
    )

    with pytest.raises(LastAdministratorError):
        users.delete_user(UUID(admin.id))

    assert users.find_user_by_id(UUID(admin.id)) is admin


def test_administrator_can_be_demoted_when_another_admin_exists(
    session: Session,
) -> None:
    organizations = OrganizationService(session)
    users = UserService(session, PasswordHasher("test-secret"))
    organization = organizations.create("Research")
    first = users.create_user(
        "first-admin",
        "first-password",
        UserRole.ADMIN,
        organization_id=UUID(organization.id),
    )
    users.create_user(
        "second-admin",
        "second-password",
        UserRole.ADMIN,
        organization_id=UUID(organization.id),
    )

    updated = users.update_role(UUID(first.id), UserRole.PUBLISHER)

    assert updated.role == UserRole.PUBLISHER.value
