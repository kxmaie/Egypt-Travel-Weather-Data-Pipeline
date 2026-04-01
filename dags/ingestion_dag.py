from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
from include.ingestion.weather_api import data_of_weather_api
from include.ingestion.landmarks_api import famous_landmark_data
from include.ingestion.csv_loader import load_resturants_csv_files_to_sql, load_hotels_csv_to_sql

local_tz = pendulum.timezone("Africa/Cairo")
default_args = {
    "owner": "momen",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 3, 4, tz=local_tz),
    "retries": 1, 
    "retry_delay": timedelta(minutes=5)
}

with DAG(
  dag_id="ingestion_dag",
  description="A DAG to ingest weather data",
  schedule="*/20 * * * *",
  default_args=default_args,
  catchup=False,
  dagrun_timeout=timedelta(minutes=30)
) as dag:
    weather = PythonOperator(
        task_id="weather_ingestion",
        python_callable=data_of_weather_api
    )

    landmarks = PythonOperator(
        task_id="landmarks_ingestion",
        python_callable=famous_landmark_data
    )

    restaurants = PythonOperator(
        task_id="restaurants_ingestion",
        python_callable=load_resturants_csv_files_to_sql,
        op_kwargs={"file_path":"/usr/local/airflow/include/Egyptian_restaurants.csv"}
    )

    hotels = PythonOperator(
        task_id="hotels_ingestion",
        python_callable=load_hotels_csv_to_sql,
        op_kwargs={"file_path":"/usr/local/airflow/include/Egypt_hotels_data.csv"}
    )


    weather >> landmarks
    restaurants >> hotels