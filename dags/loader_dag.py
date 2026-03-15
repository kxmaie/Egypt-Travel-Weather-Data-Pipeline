from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

from include.loaders.snowflake_loader import transfer_table_hotels_to_snowflake,transfer_tables_resturant_to_snowflake,transfer_table_famous_landmark_to_snowflake,transfer_table_current_weather_to_snowflake,transfer_table_daily_weather_to_snowflake,transfer_table_hourly_weather_to_snowflake




local_tz = pendulum.timezone("Africa/Cairo")
default_args = {
    "owner": "momen",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 3, 4, tz=local_tz),
    "retries": 1, 
    "retry_delay": timedelta(minutes=5)
}

with DAG(
  dag_id="loader_dag",
  description="A DAG to load data into the warehouse",
  schedule="0 0 * * *",
  default_args=default_args,
  catchup=False,
  dagrun_timeout=timedelta(minutes=30)
) as dag:
    load_hotels_to_snowflake = PythonOperator(
        task_id="load_hotels_to_snowflake",
        python_callable=transfer_table_hotels_to_snowflake
    )

    load_restaurants_to_snowflake = PythonOperator(
        task_id="load_restaurants_to_snowflake",
        python_callable=transfer_tables_resturant_to_snowflake
    )

    load_landmarks_to_snowflake = PythonOperator(
        task_id="load_landmarks_to_snowflake",
        python_callable=transfer_table_famous_landmark_to_snowflake
    )

    load_current_weather_to_snowflake = PythonOperator(
        task_id="load_current_weather_to_snowflake",
        python_callable=transfer_table_current_weather_to_snowflake
    )

    load_daily_weather_to_snowflake = PythonOperator(
        task_id="load_daily_weather_to_snowflake",
        python_callable=transfer_table_daily_weather_to_snowflake
    )

    load_hourly_weather_to_snowflake = PythonOperator(
        task_id="load_hourly_weather_to_snowflake",
        python_callable=transfer_table_hourly_weather_to_snowflake
    )

[
load_hotels_to_snowflake,
load_restaurants_to_snowflake,
load_landmarks_to_snowflake,
load_current_weather_to_snowflake,
load_daily_weather_to_snowflake,
load_hourly_weather_to_snowflake
]