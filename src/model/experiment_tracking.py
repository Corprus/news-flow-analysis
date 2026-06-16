from __future__ import annotations

from pathlib import Path

import pandas as pd

from .data import save_prediction_csv
from .evaluation import add_experiment_result, compact_metrics_table, evaluate_predictions


class ExperimentTracker:
    """Минимальный трекер экспериментов для ноутбука.

    Он сохраняет prediction CSV, считает метрики на golden и ведёт единую таблицу.
    """

    def __init__(self, predictions_dir: str | Path) -> None:
        self.predictions_dir = Path(predictions_dir)
        self.results_table = pd.DataFrame()

    def remove(self, experiment: str) -> None:
        if self.results_table.empty or "experiment" not in self.results_table.columns:
            return
        self.results_table = (
            self.results_table[self.results_table["experiment"] != experiment]
            .copy()
            .reset_index(drop=True)
        )

    def register(
        self,
        *,
        experiment: str,
        golden: pd.DataFrame,
        prediction: pd.DataFrame,
        comment: str = "",
        **metadata,
    ) -> dict:
        self.remove(experiment)
        prediction_path = self.predictions_dir / f"{experiment}.csv"
        save_prediction_csv(prediction, prediction_path)
        metrics = evaluate_predictions(golden, prediction)
        self.results_table = add_experiment_result(
            self.results_table,
            experiment_name=experiment,
            metrics=metrics,
            prediction_path=str(prediction_path),
            comment=comment,
            **metadata,
        )
        return metrics

    def compact(self) -> pd.DataFrame:
        return compact_metrics_table(self.results_table)
