# Быстрый запуск

Рекомендуемый способ знакомства с проектом — демонстрационный стек Docker
Compose с GPU-воркером.

## Требования

- Docker Desktop или Docker Engine с Compose;
- NVIDIA GPU и NVIDIA Container Toolkit для GPU-режима;
- свободные порты `80` и `15672`;
- несколько гигабайт диска для образов и модели BGE-M3.

CPU-вариант описан в [развёртывании](deployment.md).

## Настройка

Создайте локальный файл окружения:

```powershell
Copy-Item .env.example .env
```

Замените пароли и секреты. Для воспроизводимого демо включите:

```text
DEMO_MODE=true
MODEL_SERVICE_GPU_REPLICAS=1
MODEL_SERVICE_CPU_REPLICAS=0
PIPELINE_DEVICE=cuda
```

Если BGE-M3 уже загружена на хост, можно подключить существующий кеш:

```text
MODEL_SERVICE_HF_CACHE=E:/MLCache/huggingface
```

## Запуск

```powershell
docker compose up --build -d
docker compose ps
```

При первом запуске `model-service` может несколько минут загружать модель.

После готовности:

- UI: <http://localhost/>
- OpenAPI: <http://localhost/api/docs>
- API health: <http://localhost/api/health>
- RabbitMQ management: <http://localhost:15672/>

## Демонстрационные учётные записи

Значения задаются через `.env`. По умолчанию:

```text
publisher: demo / demo12345
admin:     admin / admin12345
```

Демонстрационный режим пересоздаёт прикладную схему и загружает данные при
старте API. Не используйте `DEMO_MODE=true` для сохраняемого окружения.

## Проверка

```powershell
python scripts/demo_smoke_test.py
```

Скрипт проверяет пользователей, организации, обработку публикаций и
семантический поиск.

## Остановка

```powershell
docker compose down
```

Команда сохраняет volumes. `docker compose down -v` удаляет данные PostgreSQL,
RabbitMQ и управляемый кеш модели.

## Дальше

- [Архитектура](architecture.md)
- [HTTP API](api.md)
- [Развёртывание](deployment.md)
- [ML-пайплайн](ml_pipeline.md)
