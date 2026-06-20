import asyncio
from datetime import UTC, datetime
from io import BytesIO
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException, UploadFile

from news.importers import (
    MAX_IMPORT_ROWS,
    ImportedNews,
    NewsImportError,
    news_importers,
)
from news.models import NewsArticle
from news.routes import import_news
from news.service import NewsImportResult, NewsService
from users.deps import CurrentUser
from users.models import UserRole


def test_lenta_import_format_is_exposed_by_registry() -> None:
    formats = news_importers.list_formats()

    assert [item.id for item in formats] == ["lenta"]
    assert formats[0].file_extensions == (".csv",)


def test_lenta_csv_is_mapped_to_imported_news() -> None:
    content = (
        b"url,title,text,topic,tags,date\n"
        b"https://example.test/1,Title,Content,Technology,tag,2020-01-01\n"
    )

    articles = news_importers.parse("lenta", content)

    assert len(articles) == 1
    assert articles[0].title == "Title"
    assert articles[0].content == "Content"
    assert articles[0].topic == "Technology"
    assert articles[0].language == "ru"
    assert articles[0].published_at.tzinfo == UTC
    assert articles[0].metadata == {"tags": "tag"}


def test_lenta_csv_accepts_timezone_aware_published_at_and_news_id() -> None:
    content = (
        b"news_id,title,text,published_at\n"
        b"42,Title,Content,2020-01-01T12:30:00+03:00\n"
    )

    article = news_importers.parse("lenta", content)[0]

    assert article.external_id == "42"
    assert article.published_at.isoformat() == "2020-01-01T12:30:00+03:00"


@pytest.mark.parametrize(
    "content, message",
    [
        (b"", "empty"),
        (b"title,text\nTitle,Content\n", "date or published_at"),
        (b"title,text,date\n,Content,2020-01-01\n", "title is empty"),
        (b"title,text,date\nTitle,Content,not-a-date\n", "invalid published date"),
    ],
)
def test_lenta_csv_reports_contract_errors(content: bytes, message: str) -> None:
    with pytest.raises(NewsImportError, match=message):
        news_importers.parse("lenta", content)


def test_unknown_import_format_is_rejected() -> None:
    with pytest.raises(NewsImportError, match="Unsupported"):
        news_importers.parse("unknown", b"content")


def test_lenta_csv_row_limit_is_enforced() -> None:
    rows = ["title,text,date"]
    rows.extend(
        f"Title {index},Content {index},2020-01-01"
        for index in range(MAX_IMPORT_ROWS + 1)
    )

    with pytest.raises(NewsImportError, match="more than"):
        news_importers.parse("lenta", "\n".join(rows).encode())


class _ImportSession:
    def __init__(self) -> None:
        self.added: list[NewsArticle] = []

    def add(self, article: NewsArticle) -> None:
        self.added.append(article)

    def flush(self) -> None:
        for article in self.added:
            if article.id is None:
                article.id = str(uuid4())


def test_import_keeps_detected_duplicate_as_separate_draft(monkeypatch) -> None:
    session = _ImportSession()
    service = NewsService(session)  # type: ignore[arg-type]
    existing_id = str(uuid4())
    monkeypatch.setattr(
        service,
        "_find_existing_article",
        lambda *_args: SimpleNamespace(id=existing_id),
    )
    monkeypatch.setattr(service, "_add_submission", lambda *_args: None)

    result = service.import_user_articles(
        user_id=uuid4(),
        format_id="lenta",
        articles=[
            ImportedNews(
                title="Repeated title",
                content="Repeated content",
                published_at=datetime(2020, 1, 1, tzinfo=UTC),
                url="https://example.test/repeated",
            )
        ],
    )

    assert result.created_count == 1
    assert result.duplicate_count == 1
    assert len(result.article_ids) == 1
    assert result.article_ids[0] != existing_id
    assert session.added[0].extra_metadata["import"]["possible_duplicate_of"] == existing_id


class _ImportNewsServiceSpy:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def import_user_articles(self, **kwargs) -> NewsImportResult:
        assert kwargs["format_id"] == "lenta"
        assert len(kwargs["articles"]) == 1
        article_id = str(uuid4())
        return NewsImportResult(
            total_rows=1,
            created_count=1,
            duplicate_count=0,
            article_ids=[article_id],
        )

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class _FailingImportNewsServiceSpy(_ImportNewsServiceSpy):
    def publish_user_articles(self, article_ids, user_id, **kwargs):
        raise ValueError("one imported article cannot be published")


def test_import_endpoint_creates_drafts_and_commits() -> None:
    service = _ImportNewsServiceSpy()
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )
    upload = UploadFile(
        filename="news.csv",
        file=BytesIO(b"title,text,date\nTitle,Content,2020-01-01\n"),
    )

    response = asyncio.run(
        import_news(
            current_user=current_user,
            news=service,
            file=upload,
            accounting=None,
            settings=SimpleNamespace(news_add_cost=0),
            publisher=None,
            repository=None,
            import_format="lenta",
        )
    )

    assert response.total_rows == 1
    assert response.created_count == 1
    assert service.committed


def test_import_endpoint_rejects_oversized_file(monkeypatch) -> None:
    monkeypatch.setattr("news.routes.MAX_IMPORT_FILE_BYTES", 4)
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )
    upload = UploadFile(filename="news.csv", file=BytesIO(b"12345"))

    with pytest.raises(HTTPException) as error:
        asyncio.run(
            import_news(
                current_user=current_user,
                news=None,
                file=upload,
                accounting=None,
                    settings=SimpleNamespace(news_add_cost=0),
                publisher=None,
                repository=None,
                import_format="lenta",
            )
        )

    assert error.value.status_code == 413


def test_import_and_publish_rolls_back_import_when_batch_is_invalid() -> None:
    service = _FailingImportNewsServiceSpy()
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )
    upload = UploadFile(
        filename="news.csv",
        file=BytesIO(b"title,text,date\nTitle,Content,2020-01-01\n"),
    )

    with pytest.raises(HTTPException) as error:
        asyncio.run(
            import_news(
                current_user=current_user,
                news=service,
                file=upload,
                accounting=None,
                settings=SimpleNamespace(news_add_cost=0),
                publisher=None,
                repository=None,
                import_format="lenta",
                publish_immediately=True,
            )
        )

    assert error.value.status_code == 409
    assert not service.committed
    assert service.rolled_back
