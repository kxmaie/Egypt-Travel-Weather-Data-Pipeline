"""
SQL Server → Snowflake loader functions.

Each function reads a silver/bronze table from SQL Server and overwrites the
corresponding table in Snowflake RAW_DATA using write_pandas.

Naming convention in Snowflake:
  - Weather tables keep their Silver_ prefix (they come from the Silver layer).
  - Hotel / restaurant tables keep their Bronze_ prefix (loaded directly from Bronze).
  - Landmarks table keeps Silver_ prefix (comes from Silver layer).
"""

import logging
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

log = logging.getLogger(__name__)


def _load_table_to_snowflake(
    source_table: str,
    target_table: str,
    mssql_conn_id: str = "sql_server_conn",
    snowflake_conn_id: str = "conn_snowflake",
    database: str = "RENTAL_WEATHER_DB",
    schema: str = "RAW_DATA",
    extra_date_columns: list | None = None,
) -> None:
    """Generic helper: read a SQL Server table and overwrite-load it into Snowflake.

    Args:
        source_table: Table name in SQL Server to read from.
        target_table: Table name in Snowflake to write to.
        mssql_conn_id: Airflow connection ID for SQL Server.
        snowflake_conn_id: Airflow connection ID for Snowflake.
        database: Snowflake target database.
        schema: Snowflake target schema.
        extra_date_columns: Additional columns to cast to datetime beyond INGESTED_AT.
    """
    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    df = mssql_hook.get_pandas_df(f"SELECT * FROM {source_table}")
    log.info("Fetched %d rows from SQL Server table '%s'.", len(df), source_table)

    df.columns = [col.upper() for col in df.columns]

    for col in (extra_date_columns or ["INGESTED_AT"]):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = snowflake_hook.get_conn()
    write_pandas(
        df=df,
        table_name=target_table,
        conn=conn,
        database=database,
        schema=schema,
        auto_create_table=True,
        overwrite=True,
    )
    log.info(
        "Successfully loaded %d rows into Snowflake '%s.%s.%s'.",
        len(df), database, schema, target_table,
    )


# ── Weather tables ────────────────────────────────────────────────────────────
# FIX #1: أسماء الجداول في Snowflake تعكس مستوى البيانات الحقيقي (Silver)
#         بدل الاسم المضلل السابق (Bronze_*) للبيانات اللي أصلها Silver.

def transfer_table_daily_weather_to_snowflake():
    _load_table_to_snowflake(
        source_table="Silver_weather_daily_data",
        target_table="Silver_daily_weather_data",   # كان: Bronze_daily_weather_data
    )


def transfer_table_hourly_weather_to_snowflake():
    _load_table_to_snowflake(
        source_table="Silver_weather_hourly_data",
        target_table="Silver_hourly_weather_data",  # كان: Bronze_hourly_weather_data
        extra_date_columns=["INGESTED_AT", "DATE"],
    )


def transfer_table_current_weather_to_snowflake():
    _load_table_to_snowflake(
        source_table="Silver_weather_current_data",
        target_table="Silver_current_weather_data", # كان: Bronze_current_weather_data
    )


# ── Landmark table ────────────────────────────────────────────────────────────

def transfer_table_famous_landmark_to_snowflake():
    _load_table_to_snowflake(
        source_table="Silver_famous_landmark_data",
        target_table="Silver_famous_landmark_data",
    )


# ── Hotel table ───────────────────────────────────────────────────────────────

def transfer_table_hotels_to_snowflake():
    _load_table_to_snowflake(
        source_table="Bronze_hotel_data",
        target_table="Bronze_hotel_data",
    )


# ── Restaurant table ──────────────────────────────────────────────────────────

def transfer_tables_resturant_to_snowflake():
    _load_table_to_snowflake(
        source_table="Bronze_restaurant_data",
        target_table="Bronze_restaurant_data",
    )
