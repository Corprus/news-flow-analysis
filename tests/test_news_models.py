import accounting.models  # noqa: F401
import news.models  # noqa: F401
from db.database import Base


def test_news_storage_tables_are_registered() -> None:
    expected_tables = {
        "news_sources",
        "news_articles",
        "article_embeddings",
        "news_events",
        "event_articles",
        "news_search_queries",
        "accounts",
        "transactions",
    }

    assert expected_tables.issubset(Base.metadata.tables)
