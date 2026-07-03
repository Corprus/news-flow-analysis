# Разработка

## Структура репозитория

```text
src/
  api/               FastAPI application
  users/             пользователи, организации и авторизация
  accounting/        баланс и журнал операций
  news/              статьи, импорт, поиск и persistence пайплайна
  model_service/     RabbitMQ consumer и ML runtime
  final_pipeline/    full/incremental clustering и novelty
  model/             экспериментальные ML-компоненты
  db/                PostgreSQL и модели хранения
  messaging/         RabbitMQ integration
ui/                  Streamlit UI
scripts/             inference, smoke tests и benchmarks
notebooks/           EDA и ML-эксперименты
docker/              Dockerfiles и nginx
docs/                продуктовая и техническая документация
tests/               автоматические тесты
```

## Окружение

Проект ориентирован на Python 3.11. Наборы зависимостей разделены:

- `requirements-api.txt` — HTTP API;
- `requirements-model-service.txt` — vectorizer service с BGE-M3;
- `requirements-model-processor.txt` — processor service для CPU-стадий aggregate;
- `requirements-ci.txt` — тесты CI;
- `requirements-dev.txt` — инструменты разработки;
- `requirements_model_improvement.txt` — offline ML.

## Проверки

```console
ruff check .
python -m pytest
docker compose config --quiet
```

Для изменений документации дополнительно проверьте локальные Markdown-ссылки и
не дублируйте канонические таблицы метрик.

## CI/CD

`.github/workflows/ci.yml` запускается для push и pull request в `main` или
`master`:

- Ruff;
- pytest;
- проверка Docker Compose config.

Сборка API и model-service images (`model-service-vectorizer-*`,
`model-service-processor`) запускается вручную через
`workflow_dispatch` с `build_images=true`.

Чтобы блокировать merge при ошибках, в GitHub branch rules нужно сделать
обязательными проверки `Ruff`, `Tests` и `Docker Compose Config`.

## Правила данных и артефактов

Полные датасеты и generated artifacts не коммитятся. Локальная структура
описана в [data/README.md](../data/README.md).

Небольшие runtime-артефакты модели можно хранить, если они необходимы для
воспроизводимого запуска и явно документированы.

## Документация

Роли документов перечислены в [docs/README.md](README.md).

При обновлении:

- продуктовые выводы меняются в `product.md`;
- замысел и критерии прототипа — в `prototype.md`;
- метрики качества — только в `evaluation.md`;
- производительность — только в `benchmarks.md`;
- README содержит краткое резюме и ссылки;
- исторические документы не выдаются за актуальное состояние.
