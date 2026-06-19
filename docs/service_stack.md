# Service Stack

Docker Compose starts:

- `api` — stores drafts, publishes news and creates pipeline jobs;
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
