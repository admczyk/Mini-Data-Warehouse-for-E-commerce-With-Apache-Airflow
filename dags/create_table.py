from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
import pendulum
from datetime import datetime, timedelta

@task
def create_table():
    try:
        hook = PostgresHook(postgres_conn_id="postgres_db_products_etl")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY NOT NULL,
                title TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL,
                rating_rate DOUBLE PRECISION,
                rating_count INT,
                price_category TEXT,
                rating_category TEXT,
                product_segment TEXT,
                avg_price_category DOUBLE PRECISION,
                avg_rating_category DOUBLE PRECISION
                );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        raise e

local_tz = pendulum.timezone("Europe/Warsaw")

default_args = {
    "owner": "admczyk",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
}

with DAG (
    dag_id = "create_product_table",
    default_args = default_args,
    description = "DAG that creates table that data can be inserted into",
    schedule = None,
    catchup = False
) as create_products_table:
    create_table_task = create_table()
