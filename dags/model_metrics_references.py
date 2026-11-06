# model_metrics_types.py
from typing import TypedDict


class ModelCounts(TypedDict):
    """Counts of the last 7 days (check_model_metrics.sql)."""
    evaluated_count: int
    actual_fraud_count: int
    predicted_fraud_count: int
    true_positive_count: int


class ModelReferences(TypedDict):
    """Reference values of the production model (MLflow run)."""
    model_name: str
    model_version: str
    validation_recall: float
    validation_precision: float
    training_prevalence: float