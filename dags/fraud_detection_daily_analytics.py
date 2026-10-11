
import logging
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import (
    TaskGroup,
    dag,
    get_current_context,
    task,
)

from typing import Any, cast

logger = logging.getLogger(__name__)

TIMEZONE = "Europe/Paris"
REPORT_DIRECTORY = Path("/opt/airflow/logs/fraud_reports")


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
    def extract_daily_metrics() -> dict:
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

        sql = """
            SELECT
                COUNT(*) AS transaction_count,

                COUNT(p.prediction_id)
                    AS evaluated_transaction_count,

                COUNT(*) FILTER (
                    WHERE p.fraud_prediction IS TRUE
                ) AS predicted_fraud_count,

                COALESCE(
                    SUM(t.amt) FILTER (
                        WHERE p.fraud_prediction IS TRUE
                    ),
                    0
                ) AS predicted_fraud_amount

            FROM public.transactions AS t

            LEFT JOIN public.predictions AS p
                ON p.transaction_id = t.transaction_id

            WHERE t.received_at >= %(period_start)s
              AND t.received_at < %(period_end)s
        """

        hook = PostgresHook(postgres_conn_id="fraud_postgres")

        row = hook.get_first(
            sql,
            parameters={
                "period_start": period_start,
                "period_end": period_end,
            },
        )

        metrics = {
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "transaction_count": int(row[0]),
            "evaluated_transaction_count": int(row[1]),
            "predicted_fraud_count": int(row[2]),
            "predicted_fraud_amount": str(row[3]),
        }

        logger.info("Extracted daily metrics: %s", metrics)

        return metrics

    @task
    def calculate_metrics(metrics: dict) -> dict:
        """Calculate additional fraud analytics indicators."""
        evaluated_count = metrics["evaluated_transaction_count"]
        fraud_count = metrics["predicted_fraud_count"]

        metrics["predicted_fraud_rate_percent"] = (
            round(fraud_count / evaluated_count * 100, 2)
            if evaluated_count > 0
            else None
        )

        logger.info(
            "Fraud prediction rate: %s",
            metrics["predicted_fraud_rate_percent"],
        )

        return metrics

    @task
    def generate_report(metrics: dict) -> dict:
        """Generate a daily Markdown report."""
        report_date = metrics["period_start"][:10]

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

    with TaskGroup(group_id="analytics"):
        extracted_metrics = extract_daily_metrics()
        daily_metrics = calculate_metrics(cast(dict[str, Any], extracted_metrics))

    with TaskGroup(group_id="reporting"):
        report = generate_report(cast(dict[str, Any], daily_metrics))
        notify_report(cast(dict[str, Any], report))


fraud_detection_daily_analytics()
