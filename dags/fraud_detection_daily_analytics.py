
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pendulum

from airflow.sdk import (
    TaskGroup,
    dag,
    get_current_context,
    task,
)

from typing import Any, cast

from contextlib import closing

logger = logging.getLogger(__name__)

TIMEZONE = "Europe/Paris"
REPORT_DIRECTORY = Path("/opt/airflow/logs/fraud_reports")
SQL_DIRECTORY = Path(__file__).parent / "sql"


@dag(
    dag_id="fraud_detection_daily_analytics",
    description="Daily fraud analytics and reporting",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "fraud-detection",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fraud-detection", "analytics", "daily"],
)
def fraud_detection_daily_analytics():

    # ---------------------------------------------------------
    # Tasks
    # ---------------------------------------------------------

    @task
    def extract_daily_metrics() -> dict[str,Any]:
        """Extract daily transaction and fraud-prediction metrics."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # The scheduled logical date is the reference date.
        # We report on the preceding calendar day in Paris time.
        context = get_current_context()

        interval_end = context.get("data_interval_end")

        # Fallback for manually triggered runs if no interval end is available.
        if interval_end is None:
            interval_end = context.get("logical_date")

        if interval_end is None:
            raise RuntimeError(
                "Airflow did not provide data_interval_end or logical_date."
            )

        period_end = interval_end.in_timezone(TIMEZONE).start_of("day")
        period_start = period_end.subtract(days=1)
        '''period_end = (
            interval_end.in_timezone(TIMEZONE)
            .start_of("day")
            .subtract(days=1)
        )
        period_start = period_end.subtract(days=1)
        '''
        sql = (
            SQL_DIRECTORY / "extract_daily_metrics.sql"
        ).read_text(encoding="utf-8")

    
        hook = PostgresHook(postgres_conn_id="fraud_postgres")

        with closing(hook.get_conn()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    {
                        "period_start": period_start,
                        "period_end": period_end,
                    },
                )

                row_values = cursor.fetchone()

                if row_values is None:
                    raise RuntimeError(
                        "The daily metrics query returned no result."
                    )

                column_names = [
                    column[0] for column in cursor.description
                ]

                row = dict(zip(column_names, row_values))

        metrics = {
            "report_date": period_start.to_date_string(),
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "transaction_count": int(row["transaction_count"]),
            "evaluated_transaction_count": int(
                row["evaluated_transaction_count"]
            ),
            "predicted_fraud_count": int(row["predicted_fraud_count"]),
            "predicted_fraud_amount": str(
                row["predicted_fraud_amount"]
            ),
        }

        logger.info("Extracted daily metrics: %s", metrics)

        return metrics

    @task
    def calculate_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
        """Calculate derived analytics indicators."""
        evaluated_count = metrics["evaluated_transaction_count"]
        fraud_count = metrics["predicted_fraud_count"]

        metrics["predicted_fraud_rate_percent"] = (
            round(fraud_count / evaluated_count * 100, 2)
            if evaluated_count > 0
            else None
        )

        return metrics

    @task
    def persist_daily_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
        """Upsert metrics and return the persisted database row."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        context = get_current_context()

        sql = (
            SQL_DIRECTORY / "upsert_daily_fraud_metrics.sql"
        ).read_text(encoding="utf-8")

        parameters = {
            **metrics,
            "airflow_run_id": context.get("run_id"),
        }

        hook = PostgresHook(postgres_conn_id="fraud_postgres")
        row = hook.get_first(sql, parameters=parameters)

        if row is None:
            raise RuntimeError(
                "The analytics upsert returned no persisted row."
            )

        persisted_metrics = {
            "report_date": row[0].isoformat(),
            "period_start": row[1].isoformat(),
            "period_end": row[2].isoformat(),
            "transaction_count": int(row[3]),
            "evaluated_transaction_count": int(row[4]),
            "predicted_fraud_count": int(row[5]),
            "predicted_fraud_rate_percent": (
                float(row[6]) if row[6] is not None else None
            ),
            "predicted_fraud_amount": str(row[7]),
            "generated_at": row[8].isoformat(),
            "airflow_run_id": row[9],
        }

        logger.info(
            "Persisted daily metrics: %s",
            persisted_metrics,
        )

        return persisted_metrics

    @task
    def generate_report(metrics: dict) -> dict:
        """Generate a daily Markdown report."""
        report_date = metrics["report_date"]

        REPORT_DIRECTORY.mkdir(parents=True, exist_ok=True)

        report_path = REPORT_DIRECTORY / f"fraud_report_{report_date}.md"

        rate = metrics["predicted_fraud_rate_percent"]
        rate_text = f"{rate:.2f}%" if rate is not None else "N/A"

        report_content = f"""# Daily Fraud Detection Report

        - **Period start:** {metrics["period_start"]}
        - **Period end (exclusive):** {metrics["period_end"]}

        ## Transaction metrics

        - Transactions received: {metrics["transaction_count"]}
        - Transactions evaluated: {metrics["evaluated_transaction_count"]}
        - Transactions flagged as fraud: {metrics["predicted_fraud_count"]}
        - Fraud prediction rate among evaluated transactions: {rate_text}
        - Amount associated with flagged transactions: {metrics["predicted_fraud_amount"]}
        """

        report_path.write_text(report_content, encoding="utf-8")

        logger.info("Report written to %s", report_path)

        return {
            "path": str(report_path),
            "content": report_content,
        }

    @task
    def notify_report(report: dict) -> None:
        """Publish the daily report to Airflow logs."""
        logger.info(
            "Daily fraud report ready: %s\n%s",
            report["path"],
            report["content"],
        )

    # ---------------------------------------------------------
    # Task groups
    # ---------------------------------------------------------

    # Analytics group: extract -> calculate -> persist.
    with TaskGroup(group_id="analytics"):
        extracted_metrics = extract_daily_metrics()
        calculated_metrics = calculate_metrics(
            cast(dict[str, Any], extracted_metrics)
        )
        persisted_metrics = persist_daily_metrics(
            cast(dict[str, Any], calculated_metrics)
        )

    # Reporting group: report -> notification.
    with TaskGroup(group_id="reporting"):
        report = generate_report(
            cast(dict[str, Any], persisted_metrics)
        )
        notify_report(
            cast(dict[str, Any], report)
        )


fraud_detection_daily_analytics()
