# Service Stack

Docker Compose starts:

- `api` — accepts news and creates pipeline jobs;
- `model-service` — runs the final full or incremental pipeline;
- `rabbitmq` — transports pipeline and semantic-search jobs;
- `postgres` — stores articles, BGE-M3 vectors and pipeline state through `pgvector`.

The model service loads:

```text
PIPELINE_MODEL_PATH=/app/data/artifacts/models/final_exp10/final_novelty_model.joblib
PIPELINE_CONFIG_PATH=/app/data/artifacts/models/final_exp10/final_pipeline_config.json
PIPELINE_DEVICE=
```

`PIPELINE_DEVICE` may be set to `cuda` in a GPU-enabled image.

Start the stack:

```bash
docker compose up --build
```

Submit a pipeline job for existing article IDs:

```bash
curl -X POST http://localhost/api/v1/news-pipeline \
  -H "Content-Type: application/json" \
  -d '{"news_ids":["ARTICLE_UUID"],"mode":"incremental"}'
```

`mode` is `incremental` or `full`. Check status at
`/api/v1/news-pipeline/<job_id>`.
