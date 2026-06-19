# Service Stack

Docker Compose starts:

- `api` — stores drafts, publishes news and creates pipeline jobs;
- `model-service` — runs the final full or incremental pipeline;
- `rabbitmq` — transports pipeline and semantic-search jobs;
- `postgres` — stores articles, BGE-M3 vectors and pipeline state through `pgvector`.

Docker uses separate dependency sets:

- `requirements-api.txt` for the lightweight HTTP API;
- `requirements-model-service.txt` for inference dependencies.

The model-service uses the pinned
`pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime` image through Google's
`mirror.gcr.io` Docker Hub cache and requests the GPU through `gpus: all`.
The default `PIPELINE_DEVICE=cuda` keeps BGE-M3 vectorization on the GPU.
The API image remains CPU-only and lightweight.

The base registry can be overridden without editing the Dockerfile:

```text
PYTORCH_IMAGE=dockerhub1.beget.com/pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime
```

Timeweb is another compatible fallback:

```text
PYTORCH_IMAGE=dockerhub.timeweb.cloud/pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime
```

An existing host Hugging Face cache can be mounted to avoid downloading BGE-M3
again:

```text
MODEL_SERVICE_HF_CACHE=E:/MLCache/huggingface
```

If this variable is omitted, Compose uses the managed `model_cache` volume.

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

## Demo mode

The API has an explicit Python entry point:

```bash
python -m api --host 0.0.0.0 --port 8000
```

To initialize repeatable demo data and start the API:

```bash
python -m api --demo
```

To discard the current database schema before creating the demo:

```bash
python -m api --demo --drop-db
```

`--drop-db` requires `--demo`, and demo mode is rejected when `APP_ENV` is
`prod` or `production`.

With Docker Compose, use the corresponding environment parameters:

```text
DEMO_MODE=true
```

Then start or recreate the stack:

```bash
docker compose up --build
```

Demo credentials default to:

```text
Demo Research:
publisher: demo / demo12345
user:      analyst / analyst12345

Partner Analytics:
publisher: partner_publisher / partner12345
user:      partner_user / partner12345

admin: admin / admin12345
```

The credentials and initial credit are configurable through
`DEMO_USER_*`, `DEMO_ADMIN_*`, and `DEMO_INITIAL_CREDIT`.

To repeat the Docker demo from a clean database, remove only this Compose
project's containers and volumes before starting it:

```bash
docker compose down -v
docker compose up -d --no-build
```

The API container always receives `DEMO_DROP_DB=false`, so its restart cannot
erase the database. `--demo --drop-db` remains available only for an explicit
non-Docker local launch.

Demo mode imports and publishes the 250-row `data/demo/lenta_demo.csv` fixture.
The API enqueues one pipeline job for rows that are not processed yet. Search
becomes useful after model-service finishes that job.

Run the end-to-end check with:

```bash
python scripts/demo_smoke_test.py
```

Regenerate the deterministic fixture from the local prepared Lenta corpus with:

```bash
python scripts/build_demo_fixture.py
```

Measured runtime characteristics for the clean Compose startup, cold demo job,
warm full pipeline and semantic search are recorded in
[`runtime_benchmark.md`](runtime_benchmark.md).

Submit a pipeline job for existing article IDs:

```bash
curl -X POST http://localhost/api/v1/news-pipeline \
  -H "Content-Type: application/json" \
  -d '{"news_ids":["ARTICLE_UUID"],"mode":"incremental"}'
```

`mode` is `incremental` or `full`. Check status at
`/api/v1/news-pipeline/<job_id>`.

## Article processing contract

RabbitMQ transports only article IDs and the processing mode. `model-service` reads
the article payload from PostgreSQL and never fetches article content from the network.

Before publication, `news_articles` must contain non-empty `title` and `content`,
plus timezone-aware `published_at`.

Visibility and processing state are independent:

```text
visibility: draft | public
processing: not_started | pending | processing | processed | error
```

`POST /api/v1/news` creates `draft/not_started` and does not enqueue work.
`POST /api/v1/news/{article_id}/publish` changes it to `public/pending` and creates
the processing job.

Job statuses remain independent:

```text
queued -> processing -> done
                     \-> failed
```

`processed` is written only after the same database transaction has persisted:

- the BGE-M3 embedding and its model revision;
- `cluster_id` and assignment provenance;
- `novelty_label`, `p_significant` and review/late-arrival flags;
- pipeline, model and configuration versions.

The first article in a new cluster is stored as `novelty_label=significant` and
`p_significant=1.0`.

## HTTP API contracts

Adding an article:

```json
{
  "title": "Required title",
  "content": "Required extracted article content",
  "published_at": "2026-06-19T12:00:00+03:00",
  "url": "https://example.com/article",
  "canonical_url": "https://example.com/article",
  "summary": null,
  "language": "ru",
  "topic": "economy"
}
```

`published_at` must include a UTC offset. Draft creation returns `article_id`,
`visibility=draft` and `status=not_started`. Publication returns the generic
processing `job_id`.

Article history exposes the completed processing fields when available:
`cluster_id`, `novelty_label`, `novelty_score`, assignment/novelty review flags,
`late_arrival`, `processed_at` and structured `pipeline_error`.

`POST /api/v1/news-pipeline` accepts UUID values in `news_ids`. Its status endpoint
returns `queued|processing|done|failed`, the original request, result and
`created_at`/`updated_at` timestamps.

Semantic search is global. It searches all `public/processed` articles and never
filters by the user who submitted an article. Drafts are excluded from both search
and clustering context.
