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
