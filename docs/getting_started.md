# Быстрый запуск

Рекомендуемый способ знакомства с проектом — демонстрационный стек Docker
Compose с GPU-воркером.

## Требования

- Docker Desktop или Docker Engine с Compose;
- NVIDIA GPU и NVIDIA Container Toolkit для GPU-режима;
- свободные порты `80`, `3000`, `9090` и `15672`;
- несколько гигабайт диска для образов и модели BGE-M3.

CPU-вариант описан в [развёртывании](deployment.md).

## Настройка

Создайте локальный файл окружения:

Скопируйте `.env.example` в `.env`.

Замените пароли и секреты. Для воспроизводимого демо включите:

```text
DEMO_MODE=true
MODEL_SERVICE_VECTORIZER_GPU_REPLICAS=1
MODEL_SERVICE_VECTORIZER_CPU_REPLICAS=0
MODEL_SERVICE_PROCESSOR_REPLICAS=1
PIPELINE_CHUNK_SIZE=5000
PIPELINE_AGGREGATE_BATCH_SIZE=1000
PIPELINE_HISTORY_WINDOW_DAYS=30
PIPELINE_HISTORY_EXPAND_CLUSTERS=true
PIPELINE_HISTORY_CLUSTER_EXPANSION_MAX_ROWS=20000
```

Если BGE-M3 уже загружена на хост, укажите в `MODEL_SERVICE_HF_CACHE`
абсолютный путь к существующему кешу в формате вашей операционной системы.
По умолчанию используется именованный Docker volume `model_cache`.

## Запуск

```console
docker compose up --build -d
docker compose ps
```

При первом запуске `model-service-vectorizer-*` может несколько минут загружать
embedding-модель. `model-service-processor` использует CPU и обрабатывает
aggregate-пачки после готовности embeddings.

После готовности:

- UI: <http://localhost/>
- OpenAPI: <http://localhost/api/docs>
- API health: <http://localhost/api/health>
- RabbitMQ management: <http://localhost:15672/>
- Grafana: <http://localhost:3000/>
- Prometheus: <http://localhost:9090/>

## Демонстрационные учётные записи

Значения задаются через `.env`. По умолчанию:

```text
publisher: demo / demo12345
admin:     admin / admin12345
```

Демонстрационный режим пересоздаёт прикладную схему и загружает данные при
старте API. Не используйте `DEMO_MODE=true` для сохраняемого окружения.

## Проверка

```console
python scripts/demo_smoke_test.py
```

Скрипт проверяет пользователей, организации, обработку публикаций и
семантический поиск.

## Остановка

```console
docker compose down
```

Команда сохраняет volumes. `docker compose down -v` удаляет данные PostgreSQL,
RabbitMQ и управляемый кеш модели.

## Дальше

- [Архитектура](architecture.md)
- [HTTP API](api.md)
- [Развёртывание](deployment.md)
- [Мониторинг](monitoring.md)
- [ML-пайплайн](ml_pipeline.md)
