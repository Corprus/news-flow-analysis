def test_fastapi_apps_are_importable() -> None:
    from api.main import app as api_app
    from model_service.main import app as model_service_app

    assert api_app.title == "Semantic News Novelty API"
    assert model_service_app.title == "Semantic News Novelty Model Service"


def test_api_routes_do_not_have_version_prefix() -> None:
    from api.main import app

    paths = set(app.openapi()["paths"])

    assert not any(path.startswith("/v1") for path in paths)
    assert {
        "/auth/login",
        "/users/me",
        "/news",
        "/news/feed",
        "/news/feed/adjacent-dates",
        "/news-search",
        "/news-pipeline",
        "/news-pipeline/{job_id}",
        "/accounting/me/balance",
    } <= paths
