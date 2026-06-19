import accounting.models  # noqa: F401
import news.models  # noqa: F401
from db.database import Base


def test_news_storage_tables_are_registered() -> None:
    expected_tables = {
        "news_sources",
        "news_articles",
        "article_pipeline_embeddings",
        "article_pipeline_state",
        "news_article_submissions",
        "news_events",
        "event_articles",
        "news_search_queries",
        "accounts",
        "transactions",
        "organizations",
        "users",
    }

    assert expected_tables.issubset(Base.metadata.tables)


def test_user_requires_an_organization() -> None:
    organization_id = Base.metadata.tables["users"].c.organization_id

    assert organization_id.nullable is False
    assert {
        foreign_key.target_fullname for foreign_key in organization_id.foreign_keys
    } == {"organizations.id"}


def test_accounting_is_scoped_to_organization_and_tracks_actor() -> None:
    accounts = Base.metadata.tables["accounts"]
    transactions = Base.metadata.tables["transactions"]

    assert set(accounts.primary_key.columns.keys()) == {"organization_id"}
    assert {
        foreign_key.target_fullname
        for foreign_key in accounts.c.organization_id.foreign_keys
    } == {"organizations.id"}
    assert {
        foreign_key.target_fullname
        for foreign_key in transactions.c.organization_id.foreign_keys
    } == {"organizations.id"}
    assert {
        foreign_key.target_fullname
        for foreign_key in transactions.c.actor_user_id.foreign_keys
    } == {"users.id"}
    assert transactions.c.actor_user_id.nullable is True
