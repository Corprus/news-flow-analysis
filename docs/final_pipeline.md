# Финальный pipeline Semantic News Novelty

Этот документ описывает финальную версию inference pipeline после экспериментов в
`notebooks/06_model_improvement_experiments_v3.ipynb`.

## Что выбрано

Финальная конфигурация:

- embeddings: `BAAI/bge-m3`;
- базовая кластеризация: строгий `exp_00b`;
- второй проход кластеризации: `exp_10` best-candidate attach;
- novelty model: `exp_10a_current_model_on_exp10_clustering`.

`exp_10a` — это текущая сохраненная CatBoost-модель, примененная к выбранной
`exp_10` кластеризации. По актуальной таблице экспериментов она дала лучший
`significant_f1` среди `exp_10*`.

Актуальные novelty-метрики на 87 размеченных golden-строках с runtime-правилом
`cluster seed → significant`: precision `0.8471`, recall `0.9863`, F1 `0.9114`.
Улучшение относительно `exp_00b` относится к clustering (`pairwise_f1`
`0.7697 → 0.9017`); novelty-метрики при той же модели совпадают.

## Основные файлы

```text
src/model/
src/final_pipeline/
scripts/run_final_pipeline.py
scripts/benchmark_final_pipeline.py
scripts/inspect_model_artifacts.py
requirements_model_improvement.txt
```

`src/model/` содержит общую экспериментальную и runtime-логику: clustering, feature
engineering, обучение классификаторов и evaluation. `src/final_pipeline/` отвечает за
production-like orchestration и переиспользует этот код, чтобы notebook и inference не
расходились в реализации.

Готовые артефакты финального pipeline:

```text
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

В `.joblib` лежит небольшой wrapper финальной novelty-модели. Embedding-модель
`BAAI/bge-m3` в репозиторий не добавляется: она загружается через
`sentence-transformers` или берется из локального Hugging Face cache.

## Установка зависимостей

```powershell
pip install -r requirements_model_improvement.txt
```

Для GPU-инференса окружение должно содержать рабочий `torch` с CUDA. В текущих
локальных проверках использовался env `E:\Mamba\envs\ml-gpu`.

## Запуск inference

Минимальный запуск из корня проекта:

```powershell
python scripts/run_final_pipeline.py `
  --project-root . `
  --input data/prepared/lenta_clean_news.csv `
  --output data/predictions/final_pipeline_v3_predictions.csv `
  --embeddings-cache data/artifacts/embeddings/final_pipeline_v3_bge_m3.npz `
  --device cuda
```

`--model` и `--config` можно не указывать. По умолчанию будут использованы:

```text
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

Если нужно явно переопределить модель или конфиг:

```powershell
python scripts/run_final_pipeline.py `
  --project-root . `
  --input data/prepared/lenta_clean_news.csv `
  --output data/predictions/final_pipeline_v3_predictions.csv `
  --embeddings-cache data/artifacts/embeddings/final_pipeline_v3_bge_m3.npz `
  --model data/artifacts/models/final_exp10/final_novelty_model.joblib `
  --config data/artifacts/models/final_exp10/final_pipeline_config.json `
  --device cuda
```

## Benchmark

Пример benchmark на 10 000 строк:

```powershell
python scripts/benchmark_final_pipeline.py `
  --project-root . `
  --n-rows 10000 `
  --device cuda
```

Результаты сохраняются в:

```text
data/artifacts/final_pipeline_benchmark/
```

## Выходной формат

Pipeline пишет CSV со схемой:

```text
news_id
published_at
topic
title
text
cluster_id
novelty_label
comment
needs_review
p_significant
```

`novelty_label` принимает значения:

- `significant`;
- `minor`;
- `duplicate`;

Первый элемент каждого нового кластера всегда получает `significant` и
`p_significant=1.0`: это seed кластера, для которого новизна внутри кластера
гарантирована по определению.

## Production-контракт результата

Полный и инкрементальный режимы возвращают единый `PipelineResult`:

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

`predictions` содержит только строки, которые вызывающий слой должен записать:

- full — все requested ID;
- incremental — requested ID и более поздние публикации с пересчитанным novelty при
  late arrival.

`embeddings` соответствуют только `embedding_ids`. В incremental-режиме это embeddings
requested ID; матрица embeddings всей истории наружу не возвращается. `context_ids`
показывает, какие исторические строки использовались только для вычисления.

`versions` содержит `pipeline_version`, embedding model/revision, novelty model version
и config version.

`assignments` хранит как итоговое назначение, так и provenance:

```text
news_id
cluster_id
baseline_component_id
assignment_method
update_method
assignment_parent_news_id
assignment_similarity
attached_to_component_id
```

`assignment_method` является устойчивым происхождением назначения: `baseline` или
`attach`. `update_method` описывает операцию текущего запуска: например `full`,
`new_cluster`, `baseline_merge`, `cluster_merge` или `new_cluster_ambiguous`.

## Как работает pipeline

1. Входной CSV приводится к clean-like формату.
2. Для `model_text` считаются id-aware embeddings.
3. Строится строгая baseline-кластеризация `exp_00b`.
4. Строятся candidate pairs для второго прохода.
5. Применяется выбранный `exp_10` attach config:

```text
exp10_src2_sim0.75_days7_m0.03_tj0.15_num1
```

6. Для кластеризованных новостей применяется финальная novelty-модель `10a`.

## Инкрементальная обработка

`FinalNewsNoveltyPipeline` остаётся эталонным batch pipeline. Для добавления новых
публикаций без изменения ранее сохранённых `cluster_id` используется
`IncrementalNewsNoveltyPipeline`.

```python
from final_pipeline import load_incremental_pipeline

pipeline = load_incremental_pipeline(project_root=".", device="cuda")
result = pipeline.process(
    historical_news_df=history_with_cluster_ids,
    historical_embeddings=history_embeddings,
    new_news_df=news,
)
```

История обязана содержать стабильный `cluster_id`, а `historical_embeddings` должны
соответствовать её строкам. Embeddings новых строк можно передать через
`new_embeddings`; иначе они рассчитываются encoder-ом.

Новые публикации обрабатываются хронологически. Pipeline:

- сохраняет исторические назначения, кроме доказанного baseline-объединения;
- присоединяет публикацию максимум к одному кластеру;
- использует baseline-порог `0.82` и evidence-aware attach из `exp_10`;
- создаёт новый кластер, если подходящего кандидата нет;
- при малом margin между двумя кластерами создаёт новый кластер с
  `assignment_needs_review=True`;
- рассчитывает novelty на общей истории, но возвращает predictions только для новых
  публикаций.

Для выбора кластера используется двустороннее временное окно: pipeline может учитывать
публикации как до, так и после новой новости. После выбора существующего кластера
публикация считается late arrival, только если её `published_at` меньше максимального
`published_at` внутри этого выбранного кластера. Более новые публикации других
кластеров на этот признак не влияют. Для нового кластера `late_arrival=False`.

Novelty самой late-arrival новости по-прежнему рассчитывается только по более раннему
контексту.

Если новая публикация имеет baseline-связь `similarity >= 0.82` сразу с несколькими
кластерами, pipeline объединяет их. Это воспроизводит связную компоненту baseline-графа,
которую построил бы full pipeline. Неоднозначные attach-кандидаты ниже baseline-порога
не объединяются.

При объединении `result.assignments` содержит исторические строки с
`update_method="cluster_merge"`, `previous_cluster_id` и
`previous_baseline_component_id`. Вызывающий слой должен обновить их `cluster_id` и
`baseline_component_id`. Исходный `assignment_method=baseline|attach` сохраняется.
Novelty пересчитывается для всего объединённого исторического кластера.

Без объединения pipeline пересчитывает novelty всех исторических публикаций выбранного
кластера с более поздним `published_at`. `result.predictions` содержит новые и все
пересчитанные строки, а их ID перечислены в `result.updated_ids`.

## Связанные документы

- [Эксперименты по улучшению модели](model_improvement.md)
- [Основной README](../README.md)

## Что не нужно коммитить

Для работы финального pipeline не нужны:

- `data/predictions/*.csv`;
- embedding caches из `data/artifacts/embeddings/*.npz`;
- архивы `*.zip`;
- `src/final_pipeline_old/`;
- training logs `notebooks/catboost_info/`.

Эти файлы являются результатами экспериментов или локальными cache.
