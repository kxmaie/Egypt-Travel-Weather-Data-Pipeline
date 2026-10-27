"""
SQL Server → Snowflake loader functions.

Each function reads a silver/bronze table from SQL Server and overwrites the
corresponding table in Snowflake RAW_DATA using write_pandas.
"""

import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas


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

    Replaces the previously duplicated boiler-plate in every transfer function.
    """
    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    df = mssql_hook.get_pandas_df(f"SELECT * FROM {source_table}")
    print(f"Fetched {len(df)} rows from SQL Server ({source_table})")
    df.columns = [x.upper() for x in df.columns]
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
    print(f"✅ تم نقل {len(df)} صف إلى Snowflake ({target_table}) بنجاح!")


# ── Weather tables ───────────────────────────────────────────────────────────

def transfer_table_daily_weather_to_snowflake():
    _load_table_to_snowflake(
        source_table="Silver_weather_daily_data",
        target_table="Bronze_daily_weather_data",
    )


def transfer_table_hourly_weather_to_snowflake():
    _load_table_to_snowflake(
        source_table="Silver_weather_hourly_data",
        target_table="Bronze_hourly_weather_data",
        extra_date_columns=["INGESTED_AT", "DATE"],
    )


def transfer_table_current_weather_to_snowflake():
    _load_table_to_snowflake(
        source_table="Silver_weather_current_data",
        target_table="Bronze_current_weather_data",
    )


# ── Landmark table ───────────────────────────────────────────────────────────

def transfer_table_famous_landmark_to_snowflake():
    _load_table_to_snowflake(
        source_table="Silver_famous_landmark_data",
        target_table="Silver_famous_landmark_data",
    )


# ── Hotel table (was MISSING — caused ImportError in loader_dag) ────────────

def transfer_table_hotels_to_snowflake():
    _load_table_to_snowflake(
        source_table="Bronze_hotel_data",
        target_table="Bronze_hotel_data",
    )


# ── Restaurant table (was MISSING — caused ImportError in loader_dag) ──────

def transfer_tables_resturant_to_snowflake():
    _load_table_to_snowflake(
        source_table="Bronze_restaurant_data",
        target_table="Bronze_restaurant_data",
    )