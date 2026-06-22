# Запуск model-service на CPU

По умолчанию `model-service` собирается с CUDA, получает доступ ко всем доступным
GPU через `gpus: all` и запускает пайплайн с `PIPELINE_DEVICE=cuda`.

CPU-вариант полезен в двух сценариях:

1. полностью заменить GPU-воркер CPU-воркером;
2. добавить CPU-воркеры к работающему GPU-воркеру.

CPU-образ собирается отдельно. Официальный CPU-вариант PyTorch 2.7.1
распространяется через Python wheels, поэтому репозиторий содержит отдельный
`docker/model-service.cpu.Dockerfile` на основе `python:3.11-slim`.

## CPU Dockerfile

Готовый `docker/model-service.cpu.Dockerfile` устанавливает CPU-сборку PyTorch:

```dockerfile
# syntax=docker/dockerfile:1.6

FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV HF_HOME=/app/.cache/huggingface
ENV HF_HUB_DISABLE_XET=1

RUN apt-get update \
    && apt-get install --no-install-recommends -y libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    torch==2.7.1 \
    --index-url https://download.pytorch.org/whl/cpu

COPY requirements-model-service.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY data/artifacts/models/final_exp10 /app/data/artifacts/models/final_exp10

EXPOSE 8000

CMD ["uvicorn", "model_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Оба образа можно собрать заранее без запуска контейнеров:

```powershell
docker compose build model-service model-service-cpu
```

Проверить собранные образы:

```powershell
docker images news-flow-model-service
```

Собранный CPU-образ занимает примерно 581 MB. Его проверенная конфигурация:

```text
PyTorch 2.7.1+cpu
torch.cuda.is_available() == False
```

## Полный переход с GPU на CPU

Установите в `.env`:

```text
MODEL_SERVICE_GPU_REPLICAS=0
MODEL_SERVICE_CPU_REPLICAS=1
```

Соберите CPU-образ, если он ещё не собран:

```powershell
docker compose build model-service-cpu
```

Примените конфигурацию:

```powershell
docker compose up -d
```

Проверить установленную сборку PyTorch:

```powershell
docker compose exec model-service-cpu python -c "import torch; print(torch.__version__); print(torch.cuda.is_available())"
```

Для CPU-образа `torch.cuda.is_available()` должен вернуть `False`.

Чтобы вернуться на GPU, установите `MODEL_SERVICE_GPU_REPLICAS=1` и
`MODEL_SERVICE_CPU_REPLICAS=0`, затем снова выполните `docker compose up -d`.

## Одновременный запуск GPU- и CPU-воркеров

`docker-compose.yml` уже содержит сервисы `model-service` для GPU и
`model-service-cpu` для CPU. Количество их реплик задаётся в `.env`.

```text
MODEL_SERVICE_GPU_REPLICAS=1
MODEL_SERVICE_CPU_REPLICAS=3
```

Оба типа воркеров используют значение `MODEL_SERVICE_HF_CACHE`. Рекомендуется
подключать один существующий кеш Hugging Face, чтобы CPU-воркер не загружал
BGE-M3 повторно:

```text
MODEL_SERVICE_HF_CACHE=E:/MLCache/huggingface
```

Если переменная не задана, Compose подключает управляемый том `model_cache`.
Первый запуск с пустым томом может занять несколько минут и загрузить несколько
гигабайт данных до перехода healthcheck в состояние `healthy`.

Соберите оба образа без запуска:

```powershell
docker compose build model-service model-service-cpu
```

Запустите или обновите стек:

```powershell
docker compose up -d
```

Количество можно изменить в любое время. Например, оставить один GPU-воркер и
уменьшить число CPU-воркеров до двух:

```text
MODEL_SERVICE_GPU_REPLICAS=1
MODEL_SERVICE_CPU_REPLICAS=2
```

После изменения `.env` снова примените конфигурацию:

```powershell
docker compose up -d
```

Полностью отключить CPU-воркеры, не удаляя CPU-образ:

```text
MODEL_SERVICE_CPU_REPLICAS=0
```

Примените значение командой `docker compose up -d`. Compose остановит и удалит
CPU-контейнеры, но сохранит образ `news-flow-model-service:cpu` и кеш модели.

## Проверка смешанного режима

После запуска GPU- и CPU-воркеров проверьте их состояние:

```powershell
docker compose ps model-service model-service-cpu
```

Оба сервиса должны перейти в состояние `healthy`. Проверить устройства внутри
контейнеров:

```powershell
docker compose exec model-service python -c "import torch; print(torch.__version__, torch.cuda.is_available())"
docker compose exec model-service-cpu python -c "import torch; print(torch.__version__, torch.cuda.is_available())"
```

Ожидаемый результат:

```text
GPU: 2.7.1+cu128 True
CPU: 2.7.1+cpu False
```

Число подключённых обработчиков очереди можно проверить через RabbitMQ:

```powershell
docker compose exec rabbitmq rabbitmqctl list_queues name consumers messages_ready messages_unacknowledged
```

При одной GPU- и одной CPU-реплике у очереди `news_vectorization.jobs` должно
быть два consumer. Совместный режим с такой конфигурацией проверен: оба сервиса
успешно загрузили пайплайн `final-v3-provenance-v1`, прошли healthcheck и
одновременно подключились к очереди.

## Распределение заданий

GPU- и CPU-воркеры подключаются к одной durable-очереди RabbitMQ как competing
consumers. Каждое сообщение обрабатывает только один свободный воркер.
`prefetch_count=1` не позволяет одному воркеру заранее забрать несколько тяжёлых
заданий.

RabbitMQ не учитывает производительность устройства: свободный CPU-воркер может
получить задачу, которую GPU выполнил бы быстрее. Общая очередь подходит для
увеличения суммарной пропускной способности, но не гарантирует минимальную
задержку каждой задачи. Для строгой маршрутизации потребуются отдельные очереди
для CPU и GPU.

Параллельные incremental-задания также могут одновременно прочитать одно
состояние истории. Если обновления должны выполняться строго последовательно,
нужен дополнительный механизм сериализации или блокировок независимо от типа
устройства.

Официальные команды установки CPU-сборок приведены в документации
[PyTorch Previous Versions](https://pytorch.org/get-started/previous-versions/#v271).
