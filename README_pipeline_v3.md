# Semantic News Novelty — pipeline v3

Архив содержит очищенную структуру кода для ноутбука `06_model_improvement_experiments_v3.ipynb` и финального inference pipeline.

## Что изменено

- Основная экспериментальная логика вынесена в `model/`:
  - `model/attach_clustering.py` — baseline clustering, candidate pairs, best-candidate attach, silver-positive sweep.
  - `model/classifier_training.py` — подготовка train frame, train/validation split, обучение CatBoost / MLP / LogReg и подбор threshold.
  - `model/experiment_tracking.py` — единый tracker экспериментов.
- `src/final_pipeline/` больше не дублирует feature engineering и clustering; он импортирует общий код из `model/`.
- Ноутбук v3 строит более логичную цепочку:
  1. baseline `exp_00b`;
  2. выбор кластеризации;
  3. выбор final-step модели на фиксированной `exp_10` кластеризации;
  4. экспорт выбранной модели;
  5. optional ablation с fine-tuned BGE-M3.

## Как разложить в проекте

Распаковать архив в корень проекта `news-flow-analysis`, чтобы получились:

```text
model/
src/final_pipeline/
scripts/run_final_pipeline.py
scripts/inspect_model_artifacts.py
```

Ноутбук `06_model_improvement_experiments_v3.ipynb` можно положить рядом с остальными notebooks.

## Модельный артефакт

В архиве нет новой обученной MLP-модели: её нужно получить запуском notebook v3, потому что обучение зависит от локальных данных и текущего `silver/golden`.

После выбора модели ноутбук сохранит artifact примерно сюда:

```text
data/artifacts/models/final_exp10/<selected_experiment>.joblib
```

Этот `.joblib` и надо передавать в final pipeline.

## Запуск финального pipeline

Пример:

```bash
python scripts/run_final_pipeline.py ^
  --project-root . ^
  --input data/prepared/lenta_clean_news.csv ^
  --output data/predictions/final_pipeline_v3_predictions.csv ^
  --model data/artifacts/models/final_exp10/exp_10a_current_model_on_exp10_clustering.joblib ^
  --embeddings-cache data/artifacts/embeddings/final_pipeline_v3_bge_m3.npz ^
  --config data/artifacts/models/final_exp10/final_pipeline_config.json
```

## Важное ограничение

Если в notebook выбран другой эксперимент, путь к `--model` нужно заменить на фактически сохранённый `.joblib`.
