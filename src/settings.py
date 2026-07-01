from decimal import Decimal
from functools import lru_cache
from urllib.parse import quote

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = Field(default="local", alias="APP_ENV")
    demo_mode: bool = Field(default=False, alias="DEMO_MODE")
    demo_user_login: str = Field(default="demo", alias="DEMO_USER_LOGIN")
    demo_user_password: str = Field(default="demo12345", alias="DEMO_USER_PASSWORD")
    demo_admin_login: str = Field(default="admin", alias="DEMO_ADMIN_LOGIN")
    demo_admin_password: str = Field(default="admin12345", alias="DEMO_ADMIN_PASSWORD")
    demo_initial_credit: Decimal = Field(
        default=Decimal("100000.00"),
        ge=0,
        alias="DEMO_INITIAL_CREDIT",
    )
    demo_news_path: str = Field(
        default="data/demo/lenta_demo.csv",
        alias="DEMO_NEWS_PATH",
    )
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="semantic_news_novelty", alias="POSTGRES_DB")
    postgres_user: str = Field(default="semantic_news_novelty", alias="POSTGRES_USER")
    postgres_password: str = Field(alias="POSTGRES_PASSWORD")
    rabbitmq_host: str = Field(default="rabbitmq", alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, alias="RABBITMQ_PORT")
    rabbitmq_user: str = Field(default="semantic_news_novelty", alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(alias="RABBITMQ_PASSWORD")
    rabbitmq_vhost: str = Field(default="/", alias="RABBITMQ_VHOST")
    news_vectorization_queue: str = Field(
        default="news_vectorization.jobs",
        alias="NEWS_VECTORIZATION_QUEUE",
    )
    news_aggregation_queue: str = Field(
        default="news_aggregation.jobs",
        alias="NEWS_AGGREGATION_QUEUE",
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
    news_add_cost: Decimal = Field(default=Decimal("1.00"), ge=0, alias="NEWS_ADD_COST")
    news_search_cost: Decimal = Field(default=Decimal("0.00"), ge=0, alias="NEWS_SEARCH_COST")
    news_import_max_file_mib: int = Field(
        default=512,
        ge=1,
        alias="NEWS_IMPORT_MAX_FILE_MIB",
    )
    news_import_max_rows: int = Field(
        default=1_000_000,
        ge=1,
        alias="NEWS_IMPORT_MAX_ROWS",
    )
    pipeline_model_path: str = Field(
        default="data/artifacts/models/final_exp10/final_novelty_model.joblib",
        alias="PIPELINE_MODEL_PATH",
    )
    pipeline_config_path: str = Field(
        default="data/artifacts/models/final_exp10/final_pipeline_config.json",
        alias="PIPELINE_CONFIG_PATH",
    )
    pipeline_device: str | None = Field(default=None, alias="PIPELINE_DEVICE")
    pipeline_chunk_size: int = Field(
        default=5_000,
        ge=1,
        alias="PIPELINE_CHUNK_SIZE",
    )
    pipeline_aggregate_batch_size: int = Field(
        default=1_000,
        ge=1,
        alias="PIPELINE_AGGREGATE_BATCH_SIZE",
    )
    pipeline_history_window_days: int = Field(
        default=30,
        ge=0,
        alias="PIPELINE_HISTORY_WINDOW_DAYS",
    )
    pipeline_history_expand_clusters: bool = Field(
        default=True,
        alias="PIPELINE_HISTORY_EXPAND_CLUSTERS",
    )
    pipeline_history_cluster_expansion_max_rows: int = Field(
        default=20_000,
        ge=0,
        alias="PIPELINE_HISTORY_CLUSTER_EXPANSION_MAX_ROWS",
    )
    pipeline_base_story_threshold: float | None = Field(
        default=None,
        ge=0,
        le=1,
        alias="PIPELINE_BASE_STORY_THRESHOLD",
    )
    pipeline_base_story_window_days: int | None = Field(
        default=None,
        ge=0,
        alias="PIPELINE_BASE_STORY_WINDOW_DAYS",
    )
    pipeline_attach_min_similarity: float | None = Field(
        default=None,
        ge=0,
        le=1,
        alias="PIPELINE_ATTACH_MIN_SIMILARITY",
    )
    pipeline_attach_max_days: int | None = Field(
        default=None,
        ge=0,
        alias="PIPELINE_ATTACH_MAX_DAYS",
    )
    pipeline_attach_min_margin: float | None = Field(
        default=None,
        ge=0,
        le=1,
        alias="PIPELINE_ATTACH_MIN_MARGIN",
    )
    pipeline_attach_source_max_cluster_size: int | None = Field(
        default=None,
        ge=1,
        alias="PIPELINE_ATTACH_SOURCE_MAX_CLUSTER_SIZE",
    )
    pipeline_attach_title_jaccard_threshold: float | None = Field(
        default=None,
        ge=0,
        le=1,
        alias="PIPELINE_ATTACH_TITLE_JACCARD_THRESHOLD",
    )
    pipeline_attach_min_shared_numbers: int | None = Field(
        default=None,
        ge=0,
        alias="PIPELINE_ATTACH_MIN_SHARED_NUMBERS",
    )
    pipeline_attach_require_evidence: bool | None = Field(
        default=None,
        alias="PIPELINE_ATTACH_REQUIRE_EVIDENCE",
    )
    model_service_role: str = Field(default="all", alias="MODEL_SERVICE_ROLE")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

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
