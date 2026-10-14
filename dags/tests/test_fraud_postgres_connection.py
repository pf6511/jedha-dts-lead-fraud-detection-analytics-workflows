from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="test_fraud_postgres_connection",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["diagnostic"],
)
def test_fraud_postgres_connection():

    @task
    def check_postgres_connection():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="fraud_postgres")

        session = hook.get_first(
            "SELECT current_database(), current_user"
        )
        transaction_count = hook.get_first(
            "SELECT COUNT(*) FROM public.transactions"
        )[0]
        prediction_count = hook.get_first(
            "SELECT COUNT(*) FROM public.predictions"
        )[0]

        print(f"PostgreSQL session: {session}")
        print(f"Transactions: {transaction_count}")
        print(f"Predictions: {prediction_count}")

    check_postgres_connection()


test_fraud_postgres_connection()