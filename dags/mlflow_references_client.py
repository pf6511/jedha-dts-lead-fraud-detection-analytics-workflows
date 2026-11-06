"""Reference values of the production model, read from the MLflow registry.

The model checks compare the live recall, precision and prevalence with
the values of the model that carries the production alias: after a
promotion, the references follow automatically.

This module use the MLflow REST API to reduce dependencies (so no mlflow library in the Airflow image):
1. the model version carrying the alias -> its run_id
2. the run -> parameter p0 and metrics val_recall, val_precision
   (logged at the decision threshold by the training script)
xxxxx . TODO Improvement - add mlflow run constants used here like val_recall
in a model_registry contracts withoin contracts layer/repo
"""

from typing import Any

import httpx

from fraud_detection_environment import Environment
from model_metrics_references import ModelReferences

def fetch_model_references() -> ModelReferences:
    """Return the reference values of the production model."""
    environment = Environment()
    tracking_uri = environment.env_var("MLFLOW_TRACKING_URI").rstrip("/")
    model_name = environment.env_var("MODEL_NAME")
    model_alias = environment.env_var("MODEL_ALIAS")

    # Private MLflow Space
    headers = {"Authorization": f"Bearer {environment.env_var('MLFLOW_TRACKING_TOKEN')}"}

    with httpx.Client(base_url=tracking_uri, headers=headers, timeout=30.0) as client:
        # 1. Model version carrying the alias
        response = client.get(
            "/api/2.0/mlflow/registered-models/alias",
            params={"name": model_name, "alias": model_alias},
        )
        response.raise_for_status()
        model_version = response.json()["model_version"]

        # 2. and model version's training run
        response = client.get(
            "/api/2.0/mlflow/runs/get",
            params={"run_id": model_version["run_id"]},
        )
        response.raise_for_status()
        run_data = response.json()["run"]["data"]

    # The API returns lists of {"key": ..., "value": ...}
    metrics = {metric["key"]: metric["value"] for metric in run_data["metrics"]}
    params = {param["key"]: param["value"] for param in run_data["params"]}

    # See Improvements in the file header
    return ModelReferences(
        model_name=model_name,
        model_version=model_version["version"],
        validation_recall=float(metrics["val_recall"]),
        validation_precision=float(metrics["val_precision"]),
        training_prevalence=float(params["p0"])
    )
