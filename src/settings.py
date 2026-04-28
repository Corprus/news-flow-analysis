from functools import lru_cache
from urllib.parse import quote

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = Field(default="local", alias="APP_ENV")
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="news_flow", alias="POSTGRES_DB")
    postgres_user: str = Field(default="news_flow", alias="POSTGRES_USER")
    postgres_password: str = Field(alias="POSTGRES_PASSWORD")
    rabbitmq_host: str = Field(default="rabbitmq", alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, alias="RABBITMQ_PORT")
    rabbitmq_user: str = Field(default="news_flow", alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(alias="RABBITMQ_PASSWORD")
    rabbitmq_vhost: str = Field(default="/", alias="RABBITMQ_VHOST")
    news_vectorization_queue: str = Field(
        default="news_vectorization.jobs",
        alias="NEWS_VECTORIZATION_QUEUE",
    )
    sqlalchemy_echo: bool = Field(default=False, alias="SQLALCHEMY_ECHO")
    password_hash_secret: str = Field(
        default="change-me-local-password-secret",
        alias="PASSWORD_HASH_SECRET",
    )
    access_token_secret: str = Field(
        default="change-me-local-access-token-secret",
        alias="ACCESS_TOKEN_SECRET",
    )
    access_token_ttl_minutes: int = Field(
        default=60 * 24,
        alias="ACCESS_TOKEN_TTL_MINUTES",
    )
    use_local_model: bool = Field(default=False, alias="USE_LOCAL_MODEL")
    local_model_source: str = Field(
        default="models/news-flow-ru-vectorization-mpnet/final",
        alias="LOCAL_MODEL_SOURCE",
    )
    remote_model_source: str = Field(
        default="configs/model_registry/latest_model.json",
        alias="REMOTE_MODEL_SOURCE",
    )

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def model_source(self) -> str:
        if self.use_local_model:
            return self.local_model_source
        return self.remote_model_source

    @property
    def database_url(self) -> str:
        user = quote(self.postgres_user, safe="")
        password = quote(self.postgres_password, safe="")
        return (
            f"postgresql://{user}:{password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def sqlalchemy_database_url(self) -> str:
        return self.database_url.replace("postgresql://", "postgresql+psycopg://", 1)

    @property
    def rabbitmq_url(self) -> str:
        user = quote(self.rabbitmq_user, safe="")
        password = quote(self.rabbitmq_password, safe="")
        vhost = quote(self.rabbitmq_vhost, safe="")
        return f"amqp://{user}:{password}@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
