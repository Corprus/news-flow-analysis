from settings import Settings


def test_settings_build_connection_urls_from_parts() -> None:
    settings = Settings(
        APP_ENV="test",
        POSTGRES_HOST="postgres",
        POSTGRES_PORT=5432,
        POSTGRES_DB="news_flow",
        POSTGRES_USER="news_flow",
        POSTGRES_PASSWORD="secret",
        RABBITMQ_HOST="rabbitmq",
        RABBITMQ_PORT=5672,
        RABBITMQ_USER="news_flow",
        RABBITMQ_PASSWORD="secret",
        RABBITMQ_VHOST="/",
    )

    assert settings.database_url == "postgresql://news_flow:secret@postgres:5432/news_flow"
    assert settings.rabbitmq_url == "amqp://news_flow:secret@rabbitmq:5672/%2F"


def test_settings_uses_remote_model_source_by_default() -> None:
    settings = Settings(
        POSTGRES_PASSWORD="secret",
        RABBITMQ_PASSWORD="secret",
        REMOTE_MODEL_SOURCE="configs/model_registry/latest_model.json",
    )

    assert settings.model_source == "configs/model_registry/latest_model.json"


def test_settings_can_switch_to_local_model_source() -> None:
    settings = Settings(
        POSTGRES_PASSWORD="secret",
        RABBITMQ_PASSWORD="secret",
        USE_LOCAL_MODEL=True,
        LOCAL_MODEL_SOURCE="models/news-flow-ru-vectorization-mpnet/final",
        REMOTE_MODEL_SOURCE="configs/model_registry/latest_model.json",
    )

    assert settings.model_source == "models/news-flow-ru-vectorization-mpnet/final"
