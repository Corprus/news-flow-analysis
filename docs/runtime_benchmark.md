# Runtime service benchmark

Измерение выполнено 20 июня 2026 года на чистом demo-запуске:

```powershell
docker compose down -v
docker compose up -d --no-build
```

После запуска не выполнялись ручные рестарты или повторная постановка задач.
У всех шести контейнеров `RestartCount=0`.

## Окружение

- Windows 11, Docker Desktop, WSL2;
- NVIDIA GeForce RTX 4070, 12 GiB VRAM;
- PyTorch `2.7.1+cu128`, CUDA runtime `12.8`;
- embedding model: `BAAI/bge-m3`;
- embedding batch size: `16`;
- локальный Hugging Face cache смонтирован через
  `MODEL_SERVICE_HF_CACHE=E:/MLCache/huggingface`;
- demo corpus: 250 публикаций Lenta.ru;
- PostgreSQL 16 + pgvector;
- RabbitMQ 3.13.

## Время старта сервисов

Готовность определялась по timestamp первого сообщения готовности в логах
контейнера. Общее время отсчитывалось от запуска первого инфраструктурного
контейнера.

| Сервис | Время от старта контейнера | Общее время от начала запуска | Критерий готовности |
|---|---:|---:|---|
| PostgreSQL | 2.23 с | 2.23 с | `database system is ready to accept connections` |
| RabbitMQ | 11.10 с | 11.11 с | `Server startup complete` |
| API | 2.93 с | 15.25 с | FastAPI `Application startup complete` |
| UI | 1.01 с | 19.07 с | Streamlit server ready |
| nginx | 0.24 с | 19.05 с | nginx configuration ready |
| model-service | 26.72 с | 44.79 с | FastAPI `Application startup complete` после загрузки BGE-M3 |

Практически это означает:

- UI и HTTP API доступны примерно через 19 секунд;
- model-service готов принимать pipeline/search jobs примерно через 45 секунд;
- первая demo pipeline job завершает обработку 250 публикаций примерно через
  56.5 секунды от начала запуска стека.

## Обработка demo-очереди

### Холодный demo job

Первая задача создаётся API до полной готовности model-service и ожидает consumer
в RabbitMQ.

| Метрика | Значение |
|---|---:|
| Режим | `incremental`, пустая история |
| Публикаций | 250 |
| Время job по PostgreSQL | 41.266 с |
| Средняя скорость с учётом ожидания холодного model-service | 6.06 публикации/с |
| Время от начала запуска стека до готовых результатов | 56.50 с |

Время job включает ожидание запуска model-service и загрузки BGE-M3. Это
пользовательская latency первого запуска, а не только вычислительное время
pipeline.

### Прогретый полный пересчёт

После загрузки модели была отправлена отдельная `full` job на те же 250
публикаций через `POST /api/v1/news-pipeline`.

| Метрика | Значение |
|---|---:|
| Серверное время по `news_pipeline_jobs` | 8.314 с |
| End-to-end время от HTTP POST до статуса `done` | 8.766 с |
| Throughput end-to-end | 28.52 публикации/с |
| Среднее время на публикацию | 35.1 мс |
| Итоговых кластеров | 173 |

После benchmark все 250 статей остались в статусе `processed`, semantic search
успешно вернул пять кластеров, API и model-service остались healthy.

## Semantic search

Две прогретые search jobs завершились за:

- 0.169 с;
- 0.120 с.

Это серверное время асинхронной задачи по timestamps PostgreSQL. Оно не включает
время UI, клиентский polling и сетевую задержку пользователя.

## Связь с offline benchmark

В [описании финального pipeline](final_pipeline.md#benchmark) и основном README
есть benchmark на 10 000 публикаций. Он измеряет standalone pipeline и подходит
для оценки устойчивого throughput на большом batch.

Этот документ измеряет другой сценарий:

- реальный Docker Compose startup;
- readiness PostgreSQL, RabbitMQ, API, UI и model-service;
- очередь RabbitMQ;
- запись embeddings и результатов в PostgreSQL;
- HTTP submit/status contract;
- холодную и прогретую latency demo.

Поэтому `28.52 публикации/с` runtime benchmark нельзя напрямую сравнивать с
offline benchmark без учёта размера batch, прогрева и сервисного persistence
overhead.

## Воспроизведение

Чистый demo startup:

```powershell
docker compose down -v
docker compose up -d --no-build
```

Проверка пользователей, организаций и semantic search:

```powershell
python scripts/demo_smoke_test.py
```

Offline benchmark на 10 000 строк:

```powershell
python scripts/benchmark_final_pipeline.py `
  --project-root . `
  --n-rows 10000 `
  --device cuda
```
