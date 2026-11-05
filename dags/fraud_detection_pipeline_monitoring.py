"""Hourly monitoring of the fraud detection pipeline.

check_* tasks  measure (SQL) and decide (monitoring_checks)
record_results stores every result in monitoring.check_results
notify_alerts  sends the failed checks to Discord (#pipeline-alerts);
               skipped when everything is ok

Connection fraud_postgres_monitoring: role fraud_detection_monitoring,
which reads the operational tables and writes only the monitoring schema.

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

from monitoring_checks import freshness_result
from pipeline_alerts_client import notify_pipeline_alerts

logger = logging.getLogger(__name__)

TIMEZONE = "Europe/Paris"
SQL_DIRECTORY = Path(__file__).parent / "sql"
CONNECTION_ID = "fraud_postgres_monitoring"


def read_sql(file_name: str) -> str:
    return (SQL_DIRECTORY / file_name).read_text(encoding="utf-8")


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
    # Flow: the next checks will be added to this list
    # ---------------------------------------------------------

    results = record_results(
        cast(list[dict[str, Any]], [check_freshness()])
    )
    notify_alerts(cast(list[dict[str, Any]], results))


fraud_detection_pipeline_monitoring()
