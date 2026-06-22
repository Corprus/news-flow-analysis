# ML-пайплайн

Документ описывает актуальный runtime-пайплайн Semantic News Novelty. История
выбора решения находится в [исследованиях](research.md), а численные результаты
— в [оценке качества](evaluation.md) и [бенчмарках](benchmarks.md).

## Задачи

Пайплайн решает три связанные задачи:

1. строит семантические представления публикаций;
2. объединяет публикации об одном инфоповоде;
3. определяет, содержит ли публикация значимое обновление.

Результаты также используются для семантического поиска и сокращения повторов в
выдаче.

## Финальная конфигурация

- embeddings: `BAAI/bge-m3`, размерность 1024;
- базовая кластеризация: `exp_00b`, similarity `0.82`, окно 14 дней;
- второй проход: `exp10_src2_sim0.75_days7_m0.03_tj0.15_num1`;
- novelty model: CatBoost-конфигурация
  `exp_10a_current_model_on_exp10_clustering`;
- novelty threshold: `0.42`;
- duplicate threshold: `0.90`;
- review margin: `0.10`.

Дообученная BGE-M3 проверялась, но не вошла в финальную конфигурацию.

## Этапы обработки

1. Входная схема нормализуется до `news_id`, `published_at`, `topic`, `title`,
   `text`.
2. Для модельного текста рассчитываются BGE-M3 embeddings.
3. Строится строгий граф семантической и временной близости.
4. Связные компоненты образуют baseline-кластеры.
5. Консервативный второй проход присоединяет публикации к лучшему кандидату.
6. Для элементов с предыдущим контекстом рассчитываются признаки новизны.
7. Классификатор и постобработка назначают `significant`, `minor` или
   `duplicate`.

Первая публикация нового кластера детерминированно получает `significant` и
`p_significant=1.0`; классификатор для неё не вызывается.

## Признаки новизны

Runtime использует 18 previous-only признаков. Они вычисляются только по более
ранним публикациям, чтобы исключить утечку из будущего. Среди них:

```text
position_in_cluster
cluster_size_so_far
max_prev_similarity
previous_centroid_similarity
title_jaccard_max
shared_numbers_count
```

## Выход

Основные пользовательские поля:

```text
cluster_id
novelty_label
p_significant
needs_review
comment
max_prev_similarity
```

`PipelineResult` дополнительно содержит:

```text
mode
requested_ids
updated_ids
context_ids
predictions
assignments
embedding_ids
embeddings
diagnostics
versions
```

Происхождение назначения включает `baseline_component_id`,
`assignment_method`, `update_method`, родительскую публикацию и similarity.

## Полный режим

`FinalNewsNoveltyPipeline` обрабатывает набор совместно. Он используется для:

- первичной загрузки большого корпуса;
- контрольной оценки;
- периодического полного reclustering;
- воспроизводимых offline-экспериментов.

В `full` режиме вызывающий слой записывает предсказания для всех запрошенных ID.

## Инкрементальный режим

`IncrementalNewsNoveltyPipeline` принимает сохранённую историю и новые
публикации. Новые строки обрабатываются хронологически:

- сохраняются стабильные исторические назначения;
- публикация присоединяется максимум к одному кластеру;
- при отсутствии кандидата создаётся новый кластер;
- неоднозначные случаи получают `assignment_needs_review`;
- строгая baseline-связь с несколькими кластерами может объединить их;
- после late arrival пересчитывается новизна затронутой части истории.

Инкрементальный режим предназначен для небольших поступающих батчей. Он не
заменяет периодический полный пересчёт: расхождение кластеров накапливается.

## Артефакты

Runtime хранит в Git только небольшие воспроизводимые артефакты:

```text
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

Не хранятся:

- transformer checkpoints и Hugging Face cache;
- кеши embeddings `.npz`;
- большие датасеты;
- предсказания и временные результаты обучения.

## Автономный запуск

```console
python scripts/run_final_pipeline.py --project-root . --input data/prepared/lenta_clean_news.csv --output data/predictions/final_pipeline_predictions.csv --embeddings-cache data/artifacts/embeddings/final_pipeline_bge_m3.npz --device cuda
```

Для CPU замените `cuda` на `cpu`.

## Код

```text
src/model/             экспериментальные и общие ML-компоненты
src/final_pipeline/    runtime orchestration, full и incremental режимы
src/model_service/     интеграция с PostgreSQL и RabbitMQ
```

## Связанные документы

- [Оценка качества](evaluation.md)
- [Бенчмарки](benchmarks.md)
- [Исследования](research.md)
- [Архитектура](architecture.md)
