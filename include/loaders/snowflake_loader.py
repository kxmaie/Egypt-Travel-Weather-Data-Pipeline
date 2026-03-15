import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas



def transfer_table_hotels_to_snowflake():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    df=mssql_hook.get_pandas_df("SELECT * FROM Bronze_hotel_data")
    print(f"Fetched {len(df)} rows from SQL")
    df.columns = [x.upper() for x in df.columns]
    df['INGESTED_AT'] = pd.to_datetime(df['INGESTED_AT'])
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake')
    conn=snowflake_hook.get_conn()
    write_pandas(df=df, table_name="Bronze_hotel_data",
                                conn=conn,
                                database="RENTAL_WEATHER_DB", 
                                schema="RAW_DATA",
                                auto_create_table=True,
                                overwrite=True
                                )
    print(f"✅ تم نقل {len(df)} صف إلى Snowflake بنجاح!")

def transfer_tables_resturant_to_snowflake():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    df=mssql_hook.get_pandas_df("SELECT * FROM Bronze_restaurant_data")
    print(f"Fetched {len(df)} rows from SQL")

    df.columns = [x.upper() for x in df.columns]
    df['INGESTED_AT'] = pd.to_datetime(df['INGESTED_AT'])
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake')
    conn=snowflake_hook.get_conn()
    write_pandas(df=df, table_name="Bronze_restaurant_data",
                                conn=conn,
                                database="RENTAL_WEATHER_DB", 
                                schema="RAW_DATA",
                                auto_create_table=True,
                                overwrite=True
                                )
    print(f"✅ تم نقل {len(df)} صف إلى Snowflake بنجاح!")

def transfer_table_famous_landmark_to_snowflake():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    df=mssql_hook.get_pandas_df("SELECT * FROM Bronze_famous_landmark_data")
    print(f"Fetched {len(df)} rows from SQL")
    df.columns = [x.upper() for x in df.columns]
    df['INGESTED_AT'] = pd.to_datetime(df['INGESTED_AT'])
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake')
    conn=snowflake_hook.get_conn()
    write_pandas(df=df, table_name="Bronze_famous_landmark_data",
                                conn=conn,
                                database="RENTAL_WEATHER_DB", 
                                schema="RAW_DATA",
                                auto_create_table=True,
                                overwrite=True
                                )
    print(f"✅ تم نقل {len(df)} صف إلى Snowflake بنجاح!")

def transfer_table_current_weather_to_snowflake():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    df=mssql_hook.get_pandas_df("SELECT * FROM Silver_weather_current_data")
    print(f"Fetched {len(df)} rows from SQL")
    df.columns = [x.upper() for x in df.columns]
    df['INGESTED_AT'] = pd.to_datetime(df['INGESTED_AT'])
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake')
    conn=snowflake_hook.get_conn()
    write_pandas(df=df, table_name="Bronze_current_weather_data",
                                conn=conn,
                                database="RENTAL_WEATHER_DB", 
                                schema="RAW_DATA",
                                auto_create_table=True,
                                overwrite=True
                                )
    print(f"✅ تم نقل {len(df)} صف إلى Snowflake بنجاح!")


def transfer_table_daily_weather_to_snowflake():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    df=mssql_hook.get_pandas_df("SELECT * FROM Silver_weather_daily_data")
    print(f"Fetched {len(df)} rows from SQL")
    df.columns = [x.upper() for x in df.columns]
    df['INGESTED_AT'] = pd.to_datetime(df['INGESTED_AT'])
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake')
    conn=snowflake_hook.get_conn()
    write_pandas(df=df, table_name="Bronze_daily_weather_data",
                                conn=conn,
                                database="RENTAL_WEATHER_DB", 
                                schema="RAW_DATA",
                                auto_create_table=True,
                                overwrite=True
                                )
    print(f"✅ تم نقل {len(df)} صف إلى Snowflake بنجاح!")

def transfer_table_hourly_weather_to_snowflake():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    df=mssql_hook.get_pandas_df("SELECT * FROM Silver_weather_hourly_data")
    print(f"Fetched {len(df)} rows from SQL")
    df.columns = [x.upper() for x in df.columns]
    df['INGESTED_AT'] = pd.to_datetime(df['INGESTED_AT'])
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake')
    conn=snowflake_hook.get_conn()
    write_pandas(df=df, table_name="Bronze_hourly_weather_data",
                                conn=conn,
                                database="RENTAL_WEATHER_DB", 
                                schema="RAW_DATA",
                                auto_create_table=True,
                                overwrite=True
                                )
    print(f"✅ تم نقل {len(df)} صف إلى Snowflake بنجاح!")