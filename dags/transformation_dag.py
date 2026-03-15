from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pendulum

from include.transformations.weather_transform import create_silver_weather_daily_table,bronze_to_silver_daily_weather, create_silver_weather_hourly_table,bronze_to_silver_weather_hourly,create_silver_weather_current_table,bronze_to_silver_weather_current
from include.transformations.landmark_transform import create_famous_landmark_table,bronze_to_silver_famous_landmark





local_tz = pendulum.timezone("Africa/Cairo")
default_args = {
    "owner": "momen",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 3, 4, tz=local_tz),
    "retries": 1, 
    "retry_delay": timedelta(minutes=5)
}

with DAG(
  dag_id="transformation_dag",
  description="A DAG to transform data",
  schedule="0 0 * * *",
  default_args=default_args,
  catchup=False,
  dagrun_timeout=timedelta(minutes=30)
) as dag:
    wait_for_ingestion=ExternalTaskSensor(
    task_id="wait_for_ingestion",
    external_dag_id="ingestion_dag",
    external_task_id=None,
    mode="reschedule",
    poke_interval=60,
    timeout=600
)
    create_silver_daily_weather_table_task= PythonOperator(
        task_id="create_silver_daily_weather_table",
        python_callable=create_silver_weather_daily_table
    )
    
    bronze_to_silver_daily_weather_task=PythonOperator(
        task_id="bronze_to_silver_daily_weather",
        python_callable=bronze_to_silver_daily_weather
    )

    create_silver_weather_hourly_table_task=PythonOperator(
        task_id="create_silver_weather_hourly_table",
        python_callable=create_silver_weather_hourly_table
    )

    bronze_to_silver_weather_hourly_task=PythonOperator(
        task_id="bronze_to_silver_weather_hourly",
        python_callable=bronze_to_silver_weather_hourly
    )

    create_silver_weather_current_table_task=PythonOperator(
        task_id="create_silver_weather_current_table",
        python_callable=create_silver_weather_current_table
    )

    bronze_to_silver_weather_current_task=PythonOperator(
        task_id="bronze_to_silver_weather_current",
        python_callable=bronze_to_silver_weather_current
    )

    create_famous_landmark_table_task=PythonOperator(
        task_id="create_famous_landmark_table",
        python_callable=create_famous_landmark_table
    )

    bronze_to_silver_famous_landmark_task=PythonOperator(
        task_id="bronze_to_silver_famous_landmark",
        python_callable=bronze_to_silver_famous_landmark
    )
wait_for_ingestion >> [
    create_silver_daily_weather_table_task,
    create_silver_weather_hourly_table_task,
    create_silver_weather_current_table_task,
    create_famous_landmark_table_task
]

create_silver_daily_weather_table_task >> bronze_to_silver_daily_weather_task

create_silver_weather_hourly_table_task >> bronze_to_silver_weather_hourly_task

create_silver_weather_current_table_task >> bronze_to_silver_weather_current_task

create_famous_landmark_table_task >> bronze_to_silver_famous_landmark_task