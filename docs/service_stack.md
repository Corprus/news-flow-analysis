# Service Stack

Docker Compose starts:

- `nginx`: public entry point.
- `api`: external FastAPI service under `/api`.
- `model-service`: internal FastAPI service and RabbitMQ consumer.
- `rabbitmq`: job transport for news vectorization tasks.
- `postgres`: PostgreSQL with the `pgvector` extension.

The model service loads the fine-tuned `sentence-transformers` model on startup.
By default it expects the model produced by `scripts/train_embeddings.py` at:

```text
models/news-flow-ru-vectorization-mpnet/final
```

Set `MODEL_NAME_OR_PATH` to a HuggingFace repo id later when the model is published.

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
