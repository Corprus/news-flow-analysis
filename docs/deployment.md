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
- очередь: `RABBITMQ_*`, `NEWS_VECTORIZATION_QUEUE`;
- воркеры: `MODEL_SERVICE_GPU_REPLICAS`, `MODEL_SERVICE_CPU_REPLICAS`;
- модель: `PIPELINE_DEVICE`, `MODEL_SERVICE_HF_CACHE`, `HF_TOKEN`;
- внешний вход: `NGINX_PORT`.

## GPU-режим

```text
MODEL_SERVICE_GPU_REPLICAS=1
MODEL_SERVICE_CPU_REPLICAS=0
PIPELINE_DEVICE=cuda
```

GPU-образ основан на PyTorch `2.7.1`, CUDA `12.8` и cuDNN 9. Базовый registry
можно переопределить через `PYTORCH_IMAGE`.

Запуск:

```powershell
docker compose up --build -d
```

## CPU-режим

```text
MODEL_SERVICE_GPU_REPLICAS=0
MODEL_SERVICE_CPU_REPLICAS=1
```

```powershell
docker compose build model-service-cpu
docker compose up -d
```

Проверка:

```powershell
docker compose exec model-service-cpu python -c "import torch; print(torch.__version__, torch.cuda.is_available())"
```

Ожидается PyTorch `2.7.1+cpu` и `False`.

CPU существенно медленнее GPU. Большой `full` job целиком обрабатывается одним
consumer; увеличение числа реплик не ускоряет одну задачу.

## Смешанный режим

```text
MODEL_SERVICE_GPU_REPLICAS=1
MODEL_SERVICE_CPU_REPLICAS=2
```

Воркеры являются competing consumers одной очереди с `prefetch_count=1`.
RabbitMQ выдаёт задачу первому свободному consumer и не учитывает скорость
устройства. Для строгой маршрутизации нужны отдельные очереди.

Параллельные incremental jobs могут читать одинаковое состояние истории.
Если обновления должны быть строго последовательными, требуется внешняя
сериализация или блокировка.

## Кеш модели

Без настройки Compose использует volume `model_cache`. Для повторного
использования кеша хоста:

```text
MODEL_SERVICE_HF_CACHE=E:/MLCache/huggingface
```

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

## Связанные документы

- [Быстрый запуск](getting_started.md)
- [Архитектура](architecture.md)
- [Бенчмарки](benchmarks.md)
