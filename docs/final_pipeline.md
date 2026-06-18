# Финальный pipeline Semantic News Novelty

Этот документ описывает финальную версию inference pipeline после экспериментов в
`notebooks/06_model_improvement_experiments_v3.ipynb`.

## Что выбрано

Финальная конфигурация:

- embeddings: `BAAI/bge-m3`;
- базовая кластеризация: строгий `exp_00b`;
- второй проход кластеризации: `exp_10` best-candidate attach;
- novelty model: `exp_10a_current_model_on_exp10_clustering`.

`exp_10a` — это текущая сохраненная CatBoost/fallback модель, примененная к выбранной
`exp_10` кластеризации. По актуальной таблице экспериментов она дала лучший
`significant_f1` среди `exp_10*`.

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
- пустое значение для первого элемента кластера без fallback-кандидатов.

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

- сохраняет все исторические назначения без изменений;
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

После вставки pipeline пересчитывает novelty всех исторических публикаций выбранного
кластера с более поздним `published_at`. Они возвращаются отдельно в
`result.updated_predictions`; prediction добавленной новости находится в
`result.predictions`. Назначения публикаций к кластерам при этом не изменяются.

Инкрементальный результат намеренно не объединяет существующие кластеры. Исправление
накопившейся фрагментации относится к отдельному batch-рекластерингу.

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
