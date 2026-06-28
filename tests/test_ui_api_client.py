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
    first.json.return_value = [{"article_id": str(index)} for index in range(10_000)]
    second = Mock()
    second.status_code = 200
    second.json.return_value = [{"article_id": str(index)} for index in range(10_000, 20_000)]
    third = Mock()
    third.status_code = 200
    third.json.return_value = [{"article_id": "20000"}]

    with patch(
        "ui.api_client.requests.request",
        side_effect=[first, second, third],
    ) as request:
        history = ApiClient("http://api").list_news_history()

    assert len(history) == 20_001
    assert [call.kwargs["params"]["offset"] for call in request.call_args_list] == [
        0,
        10_000,
        20_000,
    ]
