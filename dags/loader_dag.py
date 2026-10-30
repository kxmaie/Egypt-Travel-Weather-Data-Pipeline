import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta
import pendulum

from include.loaders.snowflake_loader import (
    transfer_table_hotels_to_snowflake,
    transfer_tables_resturant_to_snowflake,
    transfer_table_famous_landmark_to_snowflake,
    transfer_table_current_weather_to_snowflake,
    transfer_table_daily_weather_to_snowflake,
    transfer_table_hourly_weather_to_snowflake,
)

log = logging.getLogger(__name__)

local_tz = pendulum.timezone("Africa/Cairo")
default_args = {
    "owner": "momen",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 3, 4, tz=local_tz),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# FIX #3: جميع الـ task dependencies جوه الـ `with DAG` block صح
with DAG(
    dag_id="loader_dag",
    description="A DAG to load Silver/Bronze data into Snowflake",
    default_args=default_args,
    catchup=False,
    schedule="@daily",
    dagrun_timeout=timedelta(minutes=30),
    tags=["etl", "load", "snowflake", "weather_hospitality"],
) as dag:

    wait_for_transform = ExternalTaskSensor(
        task_id="wait_for_transform",
        external_dag_id="transformation_dag",
        execution_delta=timedelta(minutes=0),
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=1500,
    )

    load_hotels_to_snowflake = PythonOperator(
        task_id="load_hotels_to_snowflake",
        python_callable=transfer_table_hotels_to_snowflake,
    )

    load_restaurants_to_snowflake = PythonOperator(
        task_id="load_restaurants_to_snowflake",
        python_callable=transfer_tables_resturant_to_snowflake,
    )

    load_landmarks_to_snowflake = PythonOperator(
        task_id="load_landmarks_to_snowflake",
        python_callable=transfer_table_famous_landmark_to_snowflake,
    )

    load_current_weather_to_snowflake = PythonOperator(
        task_id="load_current_weather_to_snowflake",
        python_callable=transfer_table_current_weather_to_snowflake,
    )

    load_daily_weather_to_snowflake = PythonOperator(
        task_id="load_daily_weather_to_snowflake",
        python_callable=transfer_table_daily_weather_to_snowflake,
    )

    load_hourly_weather_to_snowflake = PythonOperator(
        task_id="load_hourly_weather_to_snowflake",
        python_callable=transfer_table_hourly_weather_to_snowflake,
    )

    # Task dependencies — داخل الـ context manager
    wait_for_transform >> [
        load_hotels_to_snowflake,
        load_restaurants_to_snowflake,
        load_landmarks_to_snowflake,
        load_current_weather_to_snowflake,
        load_daily_weather_to_snowflake,
        load_hourly_weather_to_snowflake,
    ]
