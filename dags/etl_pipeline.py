from airflow import DAG
from etl.extract import get_data, save_to_json
from etl.transform import read_file, save_as_csv, transform_product_data, products_summary
from etl.load import load_data
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Europe/Warsaw")

default_args = {
    "owner": "admczyk",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
}

with DAG (
    dag_id = "etl_pipeline",
    default_args = default_args,
    description = "DAG that initializes etl pipeline",
    schedule = "@daily",
    catchup = False
) as etl_pipeline:
    extracted_data = get_data("products")
    save_data_to_json = save_to_json(extracted_data, "products")

    read_data = read_file("products")
    transformed_data = transform_product_data(read_data)
    new_transformed_data = products_summary(transformed_data)
    save_data_to_csv = save_as_csv(new_transformed_data, "products")

    data_loading = load_data("products")

