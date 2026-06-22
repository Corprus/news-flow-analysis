# Мониторинг

Docker Compose включает минимальный стек мониторинга:

- `grafana` — готовый дашборд `News Flow MVP`;
- `prometheus` — сбор и хранение временных рядов;
- `cadvisor` — CPU и RAM контейнеров;
- встроенный `rabbitmq_prometheus` — состояние очередей;
- endpoint `/metrics` у `model-service` — скорость и результаты обработки.

## Запуск

Задайте пароль администратора Grafana в `.env`:

```text
GRAFANA_ADMIN_PASSWORD=change_me
```

Запустите или пересоздайте стек:

```console
docker compose up --build -d
```

Интерфейсы:

- Grafana: <http://localhost:3000/>
- Prometheus: <http://localhost:9090/>

Порты можно изменить через `GRAFANA_PORT` и `PROMETHEUS_PORT`.

## Дашборд MVP

Дашборд создаётся автоматически и показывает:

- текущее количество новостей со статусом `processed`;
- количество готовых и обрабатываемых сообщений RabbitMQ;
- скорость успешной обработки в новостях в секунду;
- количество неуспешных pipeline jobs после старта воркера;
- p95 длительности pipeline jobs;
- CPU и RAM по сервисам Docker Compose.

Скорость рассчитывается по счётчику успешно обработанных входных статей.
Повторная обработка увеличивает этот счётчик, но не увеличивает текущее
количество строк со статусом `processed`.

## Проверка источников

В Prometheus откройте `Status → Target health`. Ожидаемые targets:

- `cadvisor`;
- `rabbitmq`;
- `model-service-gpu` или `model-service-cpu`, в зависимости от режима;
- `prometheus`.

Prometheus использует DNS service discovery и автоматически обнаруживает
запущенные реплики воркеров.

## Ограничения MVP

- метрики приложения сбрасываются при перезапуске `model-service`, исторические
  значения сохраняются в Prometheus;
- общее количество обработанных новостей восстанавливается из PostgreSQL;
- cAdvisor требует расширенный доступ к Docker host и запускается с
  `privileged: true`;
- Grafana и Prometheus опубликованы на host без TLS, поэтому для внешнего
  окружения их нужно закрыть firewall, reverse proxy или внутренней сетью.
