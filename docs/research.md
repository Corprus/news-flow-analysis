# Исследования

Документ описывает путь к текущему ML-решению. Актуальный runtime-контракт
находится в [ml_pipeline.md](ml_pipeline.md), итоговые метрики — в
[evaluation.md](evaluation.md).

## Основные ноутбуки

| Ноутбук | Назначение |
|---|---|
| `01_train_embeddings.ipynb` | ранний эксперимент с embeddings |
| `02_lenta_event_grouping.ipynb` | первичная событийная группировка |
| `03_data_analysis_and_dataset_preparation.ipynb` | EDA и подготовка данных |
| `04_evaluate_annotations_and_predictions.ipynb` | оценка разметки и предсказаний |
| `05_semantic_baseline_model.ipynb` | baseline clustering и novelty |
| `06_model_improvement_experiments_v3.ipynb` | выбор финальной конфигурации |
| `07_bronze_annotation_export.ipynb` | подготовка bronze-разметки |
| `08_finetune_bge_m3_embeddings.ipynb` | ablation с дообучением BGE-M3 |
| `09_search_and_dedup_benchmark.ipynb` | поиск и повторы в Top-10 |
| `10_incremental_vs_full_benchmark.ipynb` | replay incremental против full |

## Выбор финальной конфигурации

Главный эксперимент:

1. воспроизвёл строгий baseline `exp_00b`;
2. сравнил варианты кластеризации без переобучения novelty model;
3. настроил второй проход `exp_10` по silver-positive сигналу;
4. сравнил final-step модели на выбранных кластерах;
5. экспортировал модель и конфигурацию;
6. проверил вариант с дообученной BGE-M3.

Финальным выбран
`exp_10a_current_model_on_exp10_clustering`.

## Почему BGE-M3 не дообучается в runtime

`08_finetune_bge_m3_embeddings.ipynb` обучал BGE-M3 на русских парафразах.
Дообученный вариант уступил базовой `BAAI/bge-m3` на последующих задачах и
остался отрицательным исследовательским результатом.

Runtime использует стандартную модель и не требует локального checkpoint
дообучения.

## Разделение кода и ноутбуков

Общая логика вынесена в:

```text
src/model/
src/final_pipeline/
```

Ноутбуки должны оркестрировать эксперименты, а не содержать альтернативную
реализацию runtime-алгоритма. Это снижает риск расхождения между отчётными
метриками и сервисом.

## Что хранится в Git

Хранятся:

- код экспериментов и runtime;
- ноутбуки и небольшие отчётные таблицы;
- финальная конфигурация и небольшой novelty artifact;
- документация методики.

Не хранятся:

- кеши embeddings;
- полные transformer checkpoints;
- большие датасеты и predictions;
- `catboost_info` и временные журналы обучения.

## Следующие исследования

- независимая оценка на данных аналитического агентства;
- реальные поисковые запросы аналитиков;
- устойчивость по темам, источникам и периодам;
- объяснение фактов, повлиявших на значимость;
- оптимизация incremental-контекста и persistence;
- политика полного reclustering на нескольких временных окнах.
