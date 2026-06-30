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


def test_settings_accepts_clustering_runtime_overrides() -> None:
    settings = Settings(
        POSTGRES_PASSWORD="secret",
        RABBITMQ_PASSWORD="secret",
        PIPELINE_BASE_STORY_THRESHOLD=0.8,
        PIPELINE_BASE_STORY_WINDOW_DAYS=10,
        PIPELINE_ATTACH_MIN_SIMILARITY=0.74,
        PIPELINE_ATTACH_MAX_DAYS=9,
        PIPELINE_ATTACH_MIN_MARGIN=0.04,
        PIPELINE_ATTACH_SOURCE_MAX_CLUSTER_SIZE=3,
        PIPELINE_ATTACH_TITLE_JACCARD_THRESHOLD=0.12,
        PIPELINE_ATTACH_MIN_SHARED_NUMBERS=2,
        PIPELINE_ATTACH_REQUIRE_EVIDENCE=False,
    )

    assert settings.pipeline_base_story_threshold == 0.8
    assert settings.pipeline_base_story_window_days == 10
    assert settings.pipeline_attach_min_similarity == 0.74
    assert settings.pipeline_attach_max_days == 9
    assert settings.pipeline_attach_min_margin == 0.04
    assert settings.pipeline_attach_source_max_cluster_size == 3
    assert settings.pipeline_attach_title_jaccard_threshold == 0.12
    assert settings.pipeline_attach_min_shared_numbers == 2
    assert settings.pipeline_attach_require_evidence is False
