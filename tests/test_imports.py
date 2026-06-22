def test_fastapi_apps_are_importable() -> None:
    from api.main import app as api_app
    from model_service.main import app as model_service_app

    assert api_app.title == "Semantic News Novelty API"
    assert model_service_app.title == "Semantic News Novelty Model Service"


def _collect_route_paths(routes: list[object]) -> set[str]:
    paths: set[str] = set()

    for route in routes:
        path = getattr(route, "path", None)
        if isinstance(path, str):
            paths.add(path)

        nested_routes = getattr(route, "routes", None)
        if nested_routes is not None:
            paths |= _collect_route_paths(list(nested_routes))

    return paths


def test_api_routes_do_not_have_version_prefix() -> None:
    from api.main import app

    paths = _collect_route_paths(list(app.routes))

    assert not any(path.startswith("/v1") for path in paths)
    assert {
        "/auth/login",
        "/users/me",
        "/news",
        "/news-search",
        "/news-pipeline",
        "/news-pipeline/{job_id}",
        "/accounting/me/balance",
    } <= paths
