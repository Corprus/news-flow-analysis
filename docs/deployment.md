# Развёртывание

Основная поставка использует Docker Compose. GPU- и CPU-воркеры реализуют один
контракт и читают общую очередь RabbitMQ.

## Конфигурация

Начните с `.env.example` и обязательно замените:

- пароли PostgreSQL и RabbitMQ;
- `PASSWORD_HASH_SECRET`;
- `ACCESS_TOKEN_SECRET`;
- демонстрационные пароли, если demo включён.

Основные группы параметров:

- приложение: `APP_ENV`, `DEMO_MODE`;
- хранилище: `POSTGRES_*`;
- очередь: `RABBITMQ_*`, `NEWS_VECTORIZATION_QUEUE`, `NEWS_AGGREGATION_QUEUE`;
- воркеры: `MODEL_SERVICE_VECTORIZER_GPU_REPLICAS`, `MODEL_SERVICE_VECTORIZER_CPU_REPLICAS`, `MODEL_SERVICE_PROCESSOR_REPLICAS`;
- обработка: `PIPELINE_CHUNK_SIZE` для embeddings-пачек, `PIPELINE_AGGREGATE_BATCH_SIZE` для aggregate-пачек, `PIPELINE_HISTORY_WINDOW_DAYS` для ограничения истории по датам текущей пачки, `PIPELINE_HISTORY_EXPAND_CLUSTERS` и `PIPELINE_HISTORY_CLUSTER_EXPANSION_MAX_ROWS` для расширения окна до целых кластеров;
- модель: `MODEL_SERVICE_HF_CACHE`, `HF_TOKEN`;
- внешний вход: `NGINX_PORT`;
- мониторинг: `GRAFANA_*`, `PROMETHEUS_*`.

## GPU-режим

```text
MODEL_SERVICE_VECTORIZER_GPU_REPLICAS=1
MODEL_SERVICE_VECTORIZER_CPU_REPLICAS=0
MODEL_SERVICE_PROCESSOR_REPLICAS=1
```

GPU-образ основан на PyTorch `2.7.1`, CUDA `12.8` и cuDNN 9. Базовый registry
можно переопределить через `PYTORCH_IMAGE`.

Запуск:

```console
docker compose up --build -d
```

## CPU-режим

```text
MODEL_SERVICE_VECTORIZER_GPU_REPLICAS=0
MODEL_SERVICE_VECTORIZER_CPU_REPLICAS=1
MODEL_SERVICE_PROCESSOR_REPLICAS=1
```

```console
docker compose build model-service-vectorizer-cpu model-service-processor
docker compose up -d
```

Проверка:

```console
docker compose exec model-service-vectorizer-cpu python -c "import torch; print(torch.__version__, torch.cuda.is_available())"
```

Ожидается PyTorch `2.7.1+cpu` и `False`.

CPU существенно медленнее GPU. Большой `full` job целиком обрабатывается одним
consumer; увеличение числа реплик не ускоряет одну задачу.

Большой `incremental` job разбивается на две очереди. `PIPELINE_CHUNK_SIZE`
по умолчанию равен `5000` и задаёт размер GPU embeddings-пачки `vectorize`.
После готовности всех embeddings воркер запускает последовательные CPU-пачки
`aggregate` размером `PIPELINE_AGGREGATE_BATCH_SIZE`, по умолчанию `1000`.
Так aggregate не держит один RabbitMQ delivery дольше consumer ack timeout.
История для aggregate загружается только в окне дат текущей пачки плюс
`PIPELINE_HISTORY_WINDOW_DAYS` дней в обе стороны, по умолчанию `30`.
Если `PIPELINE_HISTORY_EXPAND_CLUSTERS=true`, строки из окна расширяются до
полных уже известных кластеров. Это снижает риск потери контекста на границе
окна без возврата к загрузке всей исторической базы. Верхняя граница
расширения задаётся `PIPELINE_HISTORY_CLUSTER_EXPANSION_MAX_ROWS`, по умолчанию
`20000`.

## Смешанный режим

```text
MODEL_SERVICE_VECTORIZER_GPU_REPLICAS=1
MODEL_SERVICE_VECTORIZER_CPU_REPLICAS=0
MODEL_SERVICE_PROCESSOR_REPLICAS=2
```

GPU-воркеры читают `NEWS_VECTORIZATION_QUEUE`, CPU-воркеры читают
`NEWS_AGGREGATION_QUEUE`. У каждой очереди остаётся `prefetch_count=1`, поэтому
vectorize и aggregate масштабируются независимо.

Параллельные incremental jobs могут читать одинаковое состояние истории.
Если обновления должны быть строго последовательными, требуется внешняя
сериализация или блокировка.

## Кеш модели

Без настройки Compose использует volume `model_cache`. Для повторного
использования кеша хоста:

Чтобы использовать существующий кеш на хосте, задайте
`MODEL_SERVICE_HF_CACHE` как абсолютный путь в формате операционной системы.
По умолчанию используется именованный Docker volume `model_cache`.

Полные transformer checkpoints не хранятся в Git. Для production-поставки
следует фиксировать ревизию модели и контрольную сумму артефактов.

## Артефакты пайплайна

В образ включаются:

```text
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

BGE-M3 загружается из Hugging Face cache или Hub.

## Поставка клиенту

Перед клиентским развёртыванием необходимо:

1. отключить `DEMO_MODE`;
2. заменить секреты и ограничить сетевой доступ;
3. определить формат и частоту импорта;
4. выбрать GPU или проверить допустимую CPU latency;
5. настроить резервное копирование PostgreSQL;
6. зафиксировать версии образов и моделей;
7. провести пилот качества на данных клиента;
8. определить расписание полного reclustering.

Текущий Compose — воспроизводимый стенд и основа поставки, но не заменяет
production-настройку TLS, secret management, backup и observability.

Минимальный мониторинг CPU, RAM, очереди и pipeline описан в
[отдельном документе](monitoring.md). Порты Grafana и Prometheus не следует
публиковать во внешнюю сеть без дополнительной защиты.

## Связанные документы

- [Быстрый запуск](getting_started.md)
- [Архитектура](architecture.md)
- [Бенчмарки](benchmarks.md)
- [Мониторинг](monitoring.md)
