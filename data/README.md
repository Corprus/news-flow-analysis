# Данные и локальные артефакты

Полные датасеты и generated outputs не хранятся в Git.

## Структура

```text
data/
  raw/          исходные датасеты
  prepared/     очищенные и подготовленные выборки
  demo/         небольшой воспроизводимый demo corpus
  artifacts/    модели, embeddings и результаты бенчмарков
  predictions/  результаты offline-инференса
```

Основной исследовательский датасет Lenta.ru ожидается по пути:

```text
data/raw/lenta.csv
```

Ожидаемые поля: `url`, `title`, `text`, `topic`, `tags`, `date`.

## Что допустимо хранить

В Git можно добавлять:

- небольшие синтетические или обезличенные примеры;
- demo fixture;
- небольшие конфигурации и runtime-артефакты, необходимые для запуска;
- метаданные воспроизводимых экспериментов.

Не следует добавлять:

- полный датасет Lenta.ru или клиентские данные;
- кеши embeddings и Hugging Face;
- большие checkpoints;
- массовые predictions и временные результаты бенчмарков.

Финальные небольшие артефакты пайплайна:

```text
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

Подробности: [ML-пайплайн](../docs/ml_pipeline.md) и
[оценка качества](../docs/evaluation.md).

## Тестовые данные

Небольшие fixtures размещаются в `tests/fixtures/`. Они не должны содержать
полные исходные или клиентские датасеты.
