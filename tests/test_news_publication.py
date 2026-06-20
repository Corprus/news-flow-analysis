import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from news.models import ArticleStatus, ArticleVisibility
from news.routes import (
    AddNewsRequest,
    DeleteNewsBatchRequest,
    NoveltyLabelUpdate,
    PublishNewsBatchRequest,
    UpdateNoveltyLabelsRequest,
    add_news,
    archive_news,
    delete_news_drafts,
    publish_news_batch,
    reprocess_news,
    restore_news,
    update_novelty_labels,
)
from users.deps import CurrentUser
from users.models import UserRole


class _Article:
    def __init__(self, article_id: str) -> None:
        self.id = article_id
        self.visibility = ArticleVisibility.PUBLIC
        self.status = ArticleStatus.PENDING


class _NewsServiceSpy:
    def __init__(self, articles: list[_Article]) -> None:
        self.articles = articles
        self.commit_calls = 0
        self.rollback_calls = 0
        self.publish_calls = []
        self.delete_calls = []
        self.archive_calls = []
        self.restore_calls = []
        self.label_calls = []
        self.reprocess_calls = []

    def publish_user_articles(self, article_ids, user_id, **kwargs):
        self.publish_calls.append((article_ids, user_id, kwargs))
        return self.articles

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def delete_user_drafts(self, article_ids, user_id):
        self.delete_calls.append((article_ids, user_id))
        return len(article_ids)

    def archive_user_articles(self, article_ids, user_id):
        self.archive_calls.append((article_ids, user_id))
        return len(article_ids)

    def restore_user_articles(self, article_ids, user_id):
        self.restore_calls.append((article_ids, user_id))
        return len(article_ids)

    def set_user_article_novelty_labels(self, labels, user_id):
        self.label_calls.append((labels, user_id))
        return len(labels)

    def prepare_user_articles_for_reprocessing(self, article_ids, user_id):
        self.reprocess_calls.append((article_ids, user_id))
        return self.articles


class _AddNewsServiceSpy(_NewsServiceSpy):
    def __init__(self, article: _Article) -> None:
        super().__init__([article])
        self.article = article

    def add_user_article(self, **kwargs):
        self.article.visibility = ArticleVisibility.DRAFT
        self.article.status = ArticleStatus.NOT_STARTED
        return self.article

    def publish_user_articles(self, article_ids, user_id, **kwargs):
        self.article.visibility = ArticleVisibility.PUBLIC
        self.article.status = ArticleStatus.PENDING
        return super().publish_user_articles(article_ids, user_id, **kwargs)


class _AccountingSpy:
    def __init__(self) -> None:
        self.withdraw_calls = []

    def withdraw_credit(self, *args) -> None:
        self.withdraw_calls.append(args)


class _FailingAccountingSpy(_AccountingSpy):
    def __init__(self, fail_on_call: int) -> None:
        super().__init__()
        self.fail_on_call = fail_on_call

    def withdraw_credit(self, *args) -> None:
        super().withdraw_credit(*args)
        if len(self.withdraw_calls) == self.fail_on_call:
            raise RuntimeError("withdrawal failed")


class _JobRepositorySpy:
    def __init__(self) -> None:
        self.queued = []

    async def mark_queued(self, job_id: str, payload: dict) -> None:
        self.queued.append((job_id, payload))


class _PublisherSpy:
    def __init__(self) -> None:
        self.messages = []

    async def publish(self, message: dict) -> None:
        self.messages.append(message)


def test_batch_publication_uses_one_pipeline_job_with_multiple_ids() -> None:
    article_ids = [uuid4(), uuid4()]
    articles = [_Article(str(article_id)) for article_id in article_ids]
    news = _NewsServiceSpy(articles)
    accounting = _AccountingSpy()
    repository = _JobRepositorySpy()
    publisher = _PublisherSpy()
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )

    response = asyncio.run(
        publish_news_batch(
            request=PublishNewsBatchRequest(article_ids=article_ids),
            current_user=current_user,
            news=news,
            accounting=accounting,
            settings=SimpleNamespace(news_add_cost=1),
            publisher=publisher,
            repository=repository,
        )
    )

    assert response.published_count == 2
    assert response.article_ids == article_ids
    assert news.commit_calls == 1
    assert len(accounting.withdraw_calls) == 2
    assert accounting.withdraw_calls[0][4] is not None
    assert accounting.withdraw_calls[0][4] == accounting.withdraw_calls[1][4]
    assert len(repository.queued) == 1
    assert repository.queued[0][1] == {
        "news_ids": [str(article_id) for article_id in article_ids],
        "mode": "incremental",
    }
    assert len(publisher.messages) == 1
    assert news.publish_calls[0][2] == {"allow_already_public": False}


def test_delete_drafts_commits_selected_articles() -> None:
    article_ids = [uuid4(), uuid4()]
    news = _NewsServiceSpy([])
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )

    response = delete_news_drafts(
        request=DeleteNewsBatchRequest(article_ids=article_ids),
        current_user=current_user,
        news=news,
    )

    assert response.deleted_count == 2
    assert news.delete_calls == [(article_ids, current_user.id)]
    assert news.commit_calls == 1


def test_archive_and_restore_commit_selected_articles() -> None:
    article_ids = [uuid4(), uuid4()]
    news = _NewsServiceSpy([])
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )
    request = DeleteNewsBatchRequest(article_ids=article_ids)

    archived = archive_news(request, current_user, news)
    restored = restore_news(request, current_user, news)

    assert archived.updated_count == 2
    assert restored.updated_count == 2
    assert news.archive_calls == [(article_ids, current_user.id)]
    assert news.restore_calls == [(article_ids, current_user.id)]
    assert news.commit_calls == 2


def test_manual_labels_are_saved_for_current_publishers_articles() -> None:
    article_ids = [uuid4(), uuid4()]
    news = _NewsServiceSpy([])
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )

    response = update_novelty_labels(
        request=UpdateNoveltyLabelsRequest(
            updates=[
                NoveltyLabelUpdate(article_id=article_ids[0], label="significant"),
                NoveltyLabelUpdate(article_id=article_ids[1], label=None),
            ]
        ),
        current_user=current_user,
        news=news,
    )

    assert response.updated_count == 2
    assert news.label_calls == [
        (
            {
                article_ids[0]: "significant",
                article_ids[1]: None,
            },
            current_user.id,
        )
    ]
    assert news.commit_calls == 1


def test_reprocess_uses_one_pipeline_job_and_charges_each_article() -> None:
    article_ids = [uuid4(), uuid4()]
    articles = [_Article(str(article_id)) for article_id in article_ids]
    news = _NewsServiceSpy(articles)
    repository = _JobRepositorySpy()
    publisher = _PublisherSpy()
    accounting = _AccountingSpy()
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )

    response = asyncio.run(
        reprocess_news(
            request=DeleteNewsBatchRequest(article_ids=article_ids),
            current_user=current_user,
            news=news,
            accounting=accounting,
            settings=SimpleNamespace(news_add_cost=1),
            publisher=publisher,
            repository=repository,
        )
    )

    assert response.queued_count == 2
    assert response.article_ids == article_ids
    assert response.status == ArticleStatus.PENDING
    assert news.reprocess_calls == [(article_ids, current_user.id)]
    assert news.commit_calls == 1
    assert len(accounting.withdraw_calls) == 2
    assert accounting.withdraw_calls[0][2].value == "news_reprocess"
    assert accounting.withdraw_calls[0][4] is not None
    assert accounting.withdraw_calls[0][4] == accounting.withdraw_calls[1][4]
    assert repository.queued[0][1] == {
        "news_ids": [str(article_id) for article_id in article_ids],
        "mode": "incremental",
    }
    assert len(publisher.messages) == 1


def test_reprocess_rolls_back_and_does_not_enqueue_when_charge_fails() -> None:
    article_ids = [uuid4(), uuid4()]
    news = _NewsServiceSpy([_Article(str(article_id)) for article_id in article_ids])
    repository = _JobRepositorySpy()
    publisher = _PublisherSpy()
    accounting = _FailingAccountingSpy(fail_on_call=2)
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )

    with pytest.raises(RuntimeError, match="withdrawal failed"):
        asyncio.run(
            reprocess_news(
                request=DeleteNewsBatchRequest(article_ids=article_ids),
                current_user=current_user,
                news=news,
                accounting=accounting,
                settings=SimpleNamespace(news_add_cost=1),
                publisher=publisher,
                repository=repository,
            )
        )

    assert news.commit_calls == 0
    assert news.rollback_calls == 1
    assert repository.queued == []
    assert publisher.messages == []


def test_manual_add_can_publish_immediately() -> None:
    article_id = uuid4()
    article = _Article(str(article_id))
    news = _AddNewsServiceSpy(article)
    accounting = _AccountingSpy()
    repository = _JobRepositorySpy()
    publisher = _PublisherSpy()
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )

    response = asyncio.run(
        add_news(
            request=AddNewsRequest(
                title="Title",
                content="Content",
                published_at="2026-01-01T12:00:00+00:00",
                publish_immediately=True,
            ),
            current_user=current_user,
            news=news,
            accounting=accounting,
            settings=SimpleNamespace(news_add_cost=1),
            publisher=publisher,
            repository=repository,
        )
    )

    assert response.article_id == article_id
    assert response.visibility == ArticleVisibility.PUBLIC
    assert response.status == ArticleStatus.PENDING
    assert response.job_id is not None
    assert len(accounting.withdraw_calls) == 1
    assert news.publish_calls[0][2] == {"allow_already_public": True}


def test_batch_publication_rolls_back_when_any_withdrawal_fails() -> None:
    article_ids = [uuid4(), uuid4()]
    news = _NewsServiceSpy([_Article(str(article_id)) for article_id in article_ids])
    accounting = _FailingAccountingSpy(fail_on_call=2)
    repository = _JobRepositorySpy()
    publisher = _PublisherSpy()
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )

    with pytest.raises(RuntimeError, match="withdrawal failed"):
        asyncio.run(
            publish_news_batch(
                request=PublishNewsBatchRequest(article_ids=article_ids),
                current_user=current_user,
                news=news,
                accounting=accounting,
                settings=SimpleNamespace(news_add_cost=1),
                publisher=publisher,
                repository=repository,
            )
        )

    assert news.commit_calls == 0
    assert news.rollback_calls == 1
    assert repository.queued == []
    assert publisher.messages == []


class _RejectingNewsServiceSpy(_NewsServiceSpy):
    def publish_user_articles(self, article_ids, user_id, **kwargs):
        raise ValueError("All articles must be publishable drafts")


def test_batch_publication_rejects_entire_mixed_set() -> None:
    news = _RejectingNewsServiceSpy([])
    repository = _JobRepositorySpy()
    publisher = _PublisherSpy()
    current_user = CurrentUser(
        id=uuid4(),
        organization_id=uuid4(),
        role=UserRole.PUBLISHER,
    )

    with pytest.raises(HTTPException) as error:
        asyncio.run(
            publish_news_batch(
                request=PublishNewsBatchRequest(article_ids=[uuid4(), uuid4()]),
                current_user=current_user,
                news=news,
                accounting=_AccountingSpy(),
                settings=SimpleNamespace(news_add_cost=1),
                publisher=publisher,
                repository=repository,
            )
        )

    assert error.value.status_code == 409
    assert news.rollback_calls == 1
    assert repository.queued == []
