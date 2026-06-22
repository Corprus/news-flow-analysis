def test_fastapi_apps_are_importable() -> None:
    from api.main import app as api_app
    from model_service.main import app as model_service_app

    assert api_app.title == "Semantic News Novelty API"
    assert model_service_app.title == "Semantic News Novelty Model Service"
