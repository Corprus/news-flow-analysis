from settings import Settings


def test_settings_build_connection_urls_from_parts() -> None:
    settings = Settings(
        APP_ENV="test",
        POSTGRES_HOST="postgres",
        POSTGRES_PORT=5432,
        POSTGRES_DB="semantic_news_novelty",
        POSTGRES_USER="semantic_news_novelty",
        POSTGRES_PASSWORD="secret",
        RABBITMQ_HOST="rabbitmq",
        RABBITMQ_PORT=5672,
        RABBITMQ_USER="semantic_news_novelty",
        RABBITMQ_PASSWORD="secret",
        RABBITMQ_VHOST="/",
    )

    assert settings.database_url == (
        "postgresql://semantic_news_novelty:secret"
        "@postgres:5432/semantic_news_novelty"
    )
    assert settings.rabbitmq_url == (
        "amqp://semantic_news_novelty:secret@rabbitmq:5672/%2F"
    )


def test_settings_uses_final_pipeline_artifacts_by_default() -> None:
    settings = Settings(
        POSTGRES_PASSWORD="secret",
        RABBITMQ_PASSWORD="secret",
    )

    assert settings.pipeline_model_path.endswith("final_novelty_model.joblib")
    assert settings.pipeline_config_path.endswith("final_pipeline_config.json")


def test_search_is_free_by_default() -> None:
    settings = Settings(
        POSTGRES_PASSWORD="secret",
        RABBITMQ_PASSWORD="secret",
    )

    assert settings.news_search_cost == 0


def test_settings_accepts_pipeline_runtime_overrides() -> None:
    settings = Settings(
        POSTGRES_PASSWORD="secret",
        RABBITMQ_PASSWORD="secret",
        PIPELINE_MODEL_PATH="/app/model.joblib",
        PIPELINE_CONFIG_PATH="/app/config.json",
        PIPELINE_DEVICE="cuda",
    )

    assert settings.pipeline_model_path == "/app/model.joblib"
    assert settings.pipeline_config_path == "/app/config.json"
    assert settings.pipeline_device == "cuda"
