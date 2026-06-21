# Стек сервисов

Docker Compose запускает:

- `api` — хранит черновики, публикует новости и создаёт задания пайплайна;
- `model-service` — выполняет финальный полный или инкрементальный пайплайн;
- `rabbitmq` — передаёт задания пайплайна и семантического поиска;
- `postgres` — хранит статьи, векторы BGE-M3 и состояние пайплайна с помощью `pgvector`.

Для Docker используются отдельные наборы зависимостей:

- `requirements-api.txt` — для облегчённого HTTP API;
- `requirements-model-service.txt` — для зависимостей инференса.

`model-service` использует зафиксированный образ
`pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime` через зеркало Docker Hub
`mirror.gcr.io` от Google и запрашивает GPU с помощью `gpus: all`.
Значение `PIPELINE_DEVICE=cuda` по умолчанию оставляет векторизацию BGE-M3 на GPU.
Образ API остаётся облегчённым и использует только CPU.

Базовый реестр можно переопределить без изменения Dockerfile:

```text
PYTORCH_IMAGE=dockerhub1.beget.com/pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime
```

Другой совместимый резервный вариант загрузки — Timeweb:

```text
PYTORCH_IMAGE=dockerhub.timeweb.cloud/pytorch/pytorch:2.7.1-cuda12.8-cudnn9-runtime
```

Чтобы не загружать BGE-M3 повторно, можно подключить существующий кеш Hugging Face с хоста:

```text
MODEL_SERVICE_HF_CACHE=E:/MLCache/huggingface
```

Если переменная не задана, Compose использует управляемый том `model_cache`.

Сервис моделей загружает:

```text
PIPELINE_MODEL_PATH=/app/data/artifacts/models/final_exp10/final_novelty_model.joblib
PIPELINE_CONFIG_PATH=/app/data/artifacts/models/final_exp10/final_pipeline_config.json
PIPELINE_DEVICE=
```

В образе с поддержкой GPU для `PIPELINE_DEVICE` можно задать значение `cuda`.

Запуск стека:

```bash
docker compose up --build
```

## Демо режим

У API есть явная точка входа Python:

```bash
python -m api --host 0.0.0.0 --port 8000
```

Чтобы инициализировать воспроизводимые демонстрационные данные и запустить API:

```bash
python -m api --demo
```

Демонстрационный режим пересоздаёт схему базы данных приложения перед загрузкой начальных данных. Запуск отклоняется, если `APP_ENV` имеет значение `prod` или
`production`.

При использовании Docker Compose задайте соответствующий параметр окружения:

```text
DEMO_MODE=true
```

Затем запустите или пересоздайте стек:

```bash
docker compose up --build
```

Учётные данные по умолчанию:

```text
Demo Research:
publisher: demo / demo12345
user:      analyst / analyst12345

Partner Analytics:
publisher: partner_publisher / partner12345
user:      partner_user / partner12345

admin: admin / admin12345
```

Учётные данные и начальный кредит настраиваются через `DEMO_USER_*`,
`DEMO_ADMIN_*` и `DEMO_INITIAL_CREDIT`.

Каждый перезапуск API с `DEMO_MODE=true` пересоздаёт схему приложения и повторно загружает демонстрационные данные. Если данные в базе должны сохраняться, используйте `DEMO_MODE=false`.

Демонстрационный режим импортирует и публикует 250 строк из
`data/demo/lenta_demo.csv`. API ставит в очередь одно задание конвейера для ещё не обработанных строк. После завершения этого задания сервисом моделей они появляются в поисковой выдаче.

Сквозная проверка:

```bash
python scripts/demo_smoke_test.py
```

Пересоздание детерминированного набора из локального подготовленного корпуса Lenta:

```bash
python scripts/build_demo_fixture.py
```

Измеренные характеристики чистого запуска Compose, холодного демонстрационного задания, прогретого полного конвейера и семантического поиска приведены в [`runtime_benchmark.md`](runtime_benchmark.md).

Отправка задания конвейера для существующих идентификаторов статей:

```bash
curl -X POST http://localhost/api/v1/news-pipeline \
  -H "Content-Type: application/json" \
  -d '{"news_ids":["ARTICLE_UUID"],"mode":"incremental"}'
```

Поле `mode` принимает значение `incremental` или `full`. Статус доступен по адресу`/api/v1/news-pipeline/<job_id>`.

## Контракт обработки статей

RabbitMQ передаёт только идентификаторы статей и режим обработки. `model-service` читает содержимое статьи из PostgreSQL и никогда не загружает его из сети.

Перед публикацией `news_articles` должна содержать непустые `title` и `content`,
а также `published_at` с указанием часового пояса.

Видимость и состояние обработки независимы:

```text
visibility: draft | public
processing: not_started | pending | processing | processed | error
```

`POST /api/v1/news` создаёт запись `draft/not_started` и не ставит задание в очередь.
`POST /api/v1/news/{article_id}/publish` переводит её в состояние `public/pending` и создаёт задание обработки.

Статусы задания также независимы:

```text
queued -> processing -> done
                     \-> failed
```

Статус `processed` записывается только после того, как в рамках той же транзакции базы данных сохранены:

- эмбеддинг BGE-M3 и ревизия его модели;
- `cluster_id` и происхождение назначения;
- `novelty_label`, `p_significant`, а также признаки проверки и позднего поступления;
- версии пайплайна, модели и конфигурации.

Первая статья нового кластера сохраняется с `novelty_label=significant` и
`p_significant=1.0`.

## Контракты HTTP API

Добавление статьи:

```json
{
  "title": "Заголовок (обязательно)",
  "content": "Текст статьи (обязательно)",
  "published_at": "2026-06-19T12:00:00+03:00",
  "url": "https://example.com/article",
  "canonical_url": "https://example.com/article",
  "summary": null,
  "language": "ru",
  "topic": "economy"
}
```

`published_at` должен содержать смещение относительно UTC. При создании черновика возвращаются `article_id`, `visibility=draft` и `status=not_started`. При публикации возвращается общий идентификатор задания обработки `job_id`.

История статьи после завершения обработки содержит доступные поля:
`cluster_id`, `novelty_label`, `novelty_score`, признаки проверки назначения и новизны, `late_arrival`, `processed_at` и структурированную ошибку `pipeline_error`.

`POST /api/v1/news-pipeline` принимает UUID в поле `news_ids`. Endpoint статуса возвращает `queued|processing|done|failed`, исходный запрос, результат и временные метки `created_at`/`updated_at`.

Семантический поиск является глобальным. Он выполняется по всем статьям в состоянии `public/processed` и не фильтрует результаты по пользователю, отправившему статью.
Черновики исключены как из поиска, так и из контекста кластеризации.
