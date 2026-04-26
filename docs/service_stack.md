# Service Stack

Docker Compose starts:

- `nginx`: public entry point.
- `api`: external FastAPI service under `/api`.
- `model-service`: internal FastAPI service and RabbitMQ consumer.
- `rabbitmq`: job transport for news vectorization tasks.
- `postgres`: PostgreSQL with the `pgvector` extension.

The model service loads the fine-tuned `sentence-transformers` model on startup.
By default it uses the remote model registry source:

```text
REMOTE_MODEL_SOURCE=/app/configs/model_registry/latest_model.json
```

Set `USE_LOCAL_MODEL=true` to use `LOCAL_MODEL_SOURCE` instead. The remote source can be
a model registry JSON or a Hugging Face model reference; the local source should be a
container-visible model directory.

Main commands:

```bash
docker compose up --build
```

Submit a news vectorization job through nginx:

```bash
curl -X POST http://localhost/api/v1/news-vectorization \
  -H "Content-Type: application/json" \
  -d "{\"text\":\"example news text\"}"
```

Check job status:

```bash
curl http://localhost/api/v1/news-vectorization/<job_id>
```

RabbitMQ management UI is exposed at `http://localhost:15672` by default.
