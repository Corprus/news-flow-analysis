from unittest.mock import Mock, patch

import pytest
from ui.api_client import ApiClient, ApiError


def test_insufficient_credits_error_is_translated() -> None:
    response = Mock()
    response.status_code = 402
    response.json.return_value = {"detail": "Insufficient credits"}

    with patch("ui.api_client.requests.request", return_value=response):
        with pytest.raises(
            ApiError,
            match="Недостаточно средств для публикации новостей",
        ) as error:
            ApiClient("http://api").import_news(
                "csv",
                "news.csv",
                b"title,content",
                publish_immediately=True,
            )

    assert error.value.status_code == 402


def test_import_job_uses_zip_content_type() -> None:
    response = Mock()
    response.status_code = 202
    response.json.return_value = {"import_job_id": "job-id", "status": "queued"}

    with patch("ui.api_client.requests.request", return_value=response) as request:
        result = ApiClient("http://api").create_news_import_job(
            "lenta",
            "news.zip",
            b"zip-content",
        )

    assert result["import_job_id"] == "job-id"
    assert request.call_args.kwargs["files"]["file"] == (
        "news.zip",
        b"zip-content",
        "application/zip",
    )


def test_list_news_history_fetches_all_pages() -> None:
    first = Mock()
    first.status_code = 200
    first.json.return_value = [{"article_id": str(index)} for index in range(500)]
    second = Mock()
    second.status_code = 200
    second.json.return_value = [{"article_id": str(index)} for index in range(500, 1_000)]
    third = Mock()
    third.status_code = 200
    third.json.return_value = [{"article_id": "1000"}]

    with patch(
        "ui.api_client.requests.request",
        side_effect=[first, second, third],
    ) as request:
        history = ApiClient("http://api").list_news_history()

    assert len(history) == 1_001
    assert [call.kwargs["params"]["offset"] for call in request.call_args_list] == [
        0,
        500,
        1_000,
    ]


def test_list_news_history_clamps_page_size_to_api_limit() -> None:
    response = Mock()
    response.status_code = 200
    response.json.return_value = []

    with patch("ui.api_client.requests.request", return_value=response) as request:
        history = ApiClient("http://api").list_news_history(page_size=10_000)

    assert history == []
    assert request.call_args.kwargs["params"] == {"limit": 500, "offset": 0}


def test_list_news_history_page_filters_visibility() -> None:
    response = Mock()
    response.status_code = 200
    response.json.return_value = [{"article_id": "draft-1"}]

    with patch("ui.api_client.requests.request", return_value=response) as request:
        history = ApiClient("http://api").list_news_history_page(
            visibility="draft",
            limit=100,
            offset=200,
        )

    assert history == [{"article_id": "draft-1"}]
    assert request.call_args.kwargs["params"] == {
        "visibility": "draft",
        "limit": 100,
        "offset": 200,
    }


def test_list_news_history_page_filters_statuses() -> None:
    response = Mock()
    response.status_code = 200
    response.json.return_value = [{"article_id": "public-1"}]

    with patch("ui.api_client.requests.request", return_value=response) as request:
        history = ApiClient("http://api").list_news_history_page(
            visibility="public",
            statuses=["pending", "processing"],
            limit=100,
            offset=0,
        )

    assert history == [{"article_id": "public-1"}]
    assert request.call_args.kwargs["params"] == {
        "visibility": "public",
        "limit": 100,
        "offset": 0,
        "status": ["pending", "processing"],
    }


def test_get_news_history_summary() -> None:
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "visibility_counts": {"draft": 0, "public": 50_050, "archived": 0},
        "status_counts": {"pending": 32_000, "processing": 18_000},
        "status_counts_by_visibility": {
            "public": {"pending": 32_000, "processing": 18_000}
        },
    }

    with patch("ui.api_client.requests.request", return_value=response) as request:
        summary = ApiClient("http://api").get_news_history_summary()

    assert summary["visibility_counts"]["public"] == 50_050
    assert request.call_args.args[:2] == ("GET", "http://api/news/me/history-summary")
