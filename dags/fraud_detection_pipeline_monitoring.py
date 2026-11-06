"""Hourly monitoring of the fraud detection pipeline.

check_* tasks  measure (SQL) and decide (monitoring_checks)
record_results stores every result in monitoring.check_results
notify_alerts  sends the failed checks to Discord (#pipeline-alerts);
               skipped when everything is ok

Connection fraud_postgres_monitoring: role fraud_detection_monitoring,
which reads the operational tables and writes only the monitoring schema.


The reference values of the model checks are read from the MLflow run
of the production model (load_model_references).

To stop the alerts while the worker is stopped on purpose, pause this
DAG in the Airflow UI.
"""

import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pendulum

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from monitoring_checks import (
    freshness_result,
    pending_failures_result,
    precision_result,
    prevalence_result,
    quarantine_rate_result,
    recall_result,
    references_unavailable_result,
)

from model_metrics_references import ModelCounts, ModelReferences

from mlflow_references_client import fetch_model_references

from pipeline_alerts_client import notify_pipeline_alerts

logger = logging.getLogger(__name__)

TIMEZONE = "Europe/Paris"
SQL_DIRECTORY = Path(__file__).parent / "sql"
CONNECTION_ID = "fraud_postgres_monitoring"


def read_sql(file_name: str) -> str:
    return (SQL_DIRECTORY / file_name).read_text(encoding="utf-8")

def read_model_counts() -> ModelCounts:
    """Counts of the last 7 days used by the three model checks."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    evaluated, actual_frauds, predicted_frauds, true_positives = hook.get_first(
        read_sql("check_model_metrics.sql")
    )
    return ModelCounts(
        evaluated_count=evaluated,
        actual_fraud_count=actual_frauds,
        predicted_fraud_count=predicted_frauds,
        true_positive_count=true_positives,
    )

@dag(
    dag_id="fraud_detection_pipeline_monitoring",
    description="Hourly health checks of the fraud detection pipeline",
    #schedule = @hourly
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "fraud-detection",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["fraud-detection", "monitoring", "hourly"],
)
def fraud_detection_pipeline_monitoring():

    # ---------------------------------------------------------
    # Checks
    # ---------------------------------------------------------

    @task
    def check_freshness() -> dict[str, Any]:
        """Minutes since the last stored transaction."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
        # type(row) = tuple p.ex (Decimal('42.0000832'),)
        row = hook.get_first(read_sql("check_freshness.sql"))
        # print(type(result))
        result = freshness_result(row[0] if row else None)
        logger.info("Freshness check: %s", result)
        return result

    @task
    def check_quarantine_rate() -> dict[str, Any]:
        """Share of the payments of the last 24 hours quarantined."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
        quarantined_count, stored_count = hook.get_first(
            read_sql("check_quarantine_rate.sql")
        )

        result = quarantine_rate_result(quarantined_count, stored_count)
        logger.info("Quarantine rate check: %s", result)
        return result

    @task
    def check_pending_failures() -> dict[str, Any]:
        """Failed payments waiting for a replay."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
        pending_count, oldest_pending_hours = hook.get_first(
            read_sql("check_pending_failures.sql")
        )

        result = pending_failures_result(pending_count, oldest_pending_hours)
        logger.info("Pending failures check: %s", result)
        return result

    # multiple_outputs=False: the task may return None
    @task(multiple_outputs=False)
    def load_model_references() -> ModelReferences | None:
        """Reference values of the production model, read from MLflow.

        Returns None if MLflow cannot be read: the model checks then
        report an alert, and the other checks still run.
        """
        try:
            references: ModelReferences | None = fetch_model_references()
        except Exception:
            logger.exception("Could not read the model references from MLflow")
            return None

        logger.info("Model references: %s", references)
        return references


    @task
    def check_recall(references: ModelReferences | None) -> dict[str, Any]:
        """Share of the real frauds detected over 7 days."""
        if references is None:
            return references_unavailable_result("recall_7d")

        counts: ModelCounts = read_model_counts()
        result = recall_result(
            counts["true_positive_count"],
            counts["actual_fraud_count"],
            reference=references["validation_recall"],
        )
        logger.info("Recall check: %s", result)
        return result

    @task
    def check_precision(references: ModelReferences | None) -> dict[str, Any]:
        """Share of the fraud alerts that were real frauds over 7 days."""
        if references is None:
            return references_unavailable_result("precision_7d")

        counts: ModelCounts = read_model_counts()
        result = precision_result(
            counts["true_positive_count"],
            counts["predicted_fraud_count"],
            reference=references["validation_precision"],
        )
        logger.info("Precision check: %s", result)
        return result

    @task
    def check_fraud_prevalence(references: ModelReferences | None) -> dict[str, Any]:
        """Share of real frauds over 7 days, compared with training."""
        if references is None:
            return references_unavailable_result("prevalence_7d")

        counts: ModelCounts = read_model_counts()
        result = prevalence_result(
            counts["actual_fraud_count"],
            counts["evaluated_count"],
            reference=references["training_prevalence"],
        )
        logger.info("Prevalence check: %s", result)
        return result
    
    # ---------------------------------------------------------
    # Results
    # ---------------------------------------------------------

    @task
    def record_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Store each check result in monitoring.check_results."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
        sql = read_sql("insert_check_result.sql")

        for result in results:
            hook.run(sql, parameters=result)

        logger.info("%d check results recorded", len(results))
        return results

    @task
    def notify_alerts(results: list[dict[str, Any]]) -> bool:
        """Send the failed checks to Discord; skip when all are ok."""
        alerts = [result for result in results if result["status"] == "alert"]

        if not alerts:
            raise AirflowSkipException("All checks ok: no alert sent.")

        sent = notify_pipeline_alerts(alerts)

        if sent:
            logger.info("%d alerts sent to Discord.", len(alerts))
        else:
            logger.warning("Failed to send %d alerts to Discord.", len(alerts))

        return sent

    # ---------------------------------------------------------
    # Flow: additional checks to be added to the list of check tasks
    # ---------------------------------------------------------

    references = cast(ModelReferences | None, load_model_references())

    results = record_results(
        cast(
            list[dict[str, Any]], [
            check_freshness(),
            check_quarantine_rate(),
            check_pending_failures(),
            check_recall(references),
            check_precision(references),
            check_fraud_prevalence(references),
            ]
        )
    )
    notify_alerts(cast(list[dict[str, Any]], results))


fraud_detection_pipeline_monitoring()
