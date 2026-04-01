import pandas as pd
import json
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def _append_silver(mssql_hook, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    df.to_sql(table, mssql_hook.get_sqlalchemy_engine(), if_exists="append", index=False)


def _delete_daily_keys(mssql_hook, df: pd.DataFrame) -> None:
    if df.empty:
        return
    cities = df["city"].unique().tolist()
    dmin, dmax = df["date"].min(), df["date"].max()
    ph = ",".join(["%s"] * len(cities))
    sql = (
        f"DELETE FROM Silver_weather_daily_data WHERE city IN ({ph}) "
        "AND date >= %s AND date <= %s"
    )
    mssql_hook.run(sql, parameters=tuple(cities) + (dmin, dmax))


def _delete_hourly_keys(mssql_hook, df: pd.DataFrame) -> None:
    if df.empty:
        return
    cities = df["city"].unique().tolist()
    dmin, dmax = df["date"].min(), df["date"].max()
    ph = ",".join(["%s"] * len(cities))
    sql = (
        f"DELETE FROM Silver_weather_hourly_data WHERE city IN ({ph}) "
        "AND date >= %s AND date <= %s"
    )
    mssql_hook.run(sql, parameters=tuple(cities) + (dmin, dmax))


def _delete_current_keys(mssql_hook, df: pd.DataFrame) -> None:
    if df.empty:
        return
    cities = df["city"].unique().tolist()
    ph = ",".join(["%s"] * len(cities))
    sql = (
        f"DELETE FROM Silver_weather_current_data WHERE city IN ({ph}) "
        "AND ingested_at >= DATEADD(day, -1, GETDATE())"
    )
    mssql_hook.run(sql, parameters=tuple(cities))


def create_silver_weather_daily_table():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    
    create_table='''
         IF OBJECT_ID('Silver_weather_daily_data','U') IS NULL
         BEGIN
             CREATE TABLE Silver_weather_daily_data(
             id INT IDENTITY(1,1) PRIMARY KEY,
             city NVARCHAR(100),
             date DATE,
             temp_max FLOAT,
             temp_min FLOAT,
             wind_speed_max FLOAT,
             weather_code INT,
             ingested_at DATETIME2
             )
        END


'''
    mssql_hook.run(create_table)
    print("✅تم إنشاء جدول Silver_weather_daily_data بنجاح.")

def bronze_to_silver_daily_weather():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    select_sql='''
     SELECT  city, raw_data, ingested_at
     FROM Bronze_weather_raw
     WHERE ingested_at >= DATEADD(day, -1, GETDATE())
    '''
    df=pd.read_sql(select_sql,mssql_hook.get_sqlalchemy_engine())
    records=[]
    for _, row in df.iterrows():
        data=json.loads(row["raw_data"])
        daily=data["daily"]
        for i in range(len(daily["time"])):
            records.append({
                "city":row["city"],
                "date":daily["time"][i],
                "temp_max": daily["temperature_2m_max"][i],
                "temp_min": daily["temperature_2m_min"][i],
                "wind_speed_max": daily["wind_speed_10m_max"][i],
                "weather_code": daily["weather_code"][i],
                "ingested_at": row["ingested_at"]
            })
    df_silver = pd.DataFrame(records)
    if not df_silver.empty:
        df_silver["date"] = pd.to_datetime(df_silver["date"]).dt.date
        df_silver = df_silver.drop_duplicates(subset=["city", "date"], keep="last")
    _delete_daily_keys(mssql_hook, df_silver)
    _append_silver(mssql_hook, "Silver_weather_daily_data", df_silver)
    print("تم نقل البيانات من جدول Bronze_weather_raw إلى جدول Silver_weather_data بنجاح.")

def create_silver_weather_hourly_table():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    create_table='''
         IF OBJECT_ID('Silver_weather_hourly_data','U') IS NULL
         BEGIN
             CREATE TABLE Silver_weather_hourly_data(
             id INT IDENTITY(1,1) PRIMARY KEY,
             city NVARCHAR(100),
             date DATETIME2,
             temp FLOAT,
             humidity INT,
             rain BIT,
             showers BIT,
             snowfall BIT,
             visibility INT,
             wind_speed FLOAT,
             weather_code INT,
             ingested_at DATETIME2
             )
        END
'''
    mssql_hook.run(create_table)
    print("✅تم إنشاء جدول Silver_weather_hourly_data بنجاح.")

def bronze_to_silver_weather_hourly():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    select_sql='''
     SELECT  city, raw_data, ingested_at
     FROM Bronze_weather_raw
     WHERE ingested_at >= DATEADD(day, -1, GETDATE())
    '''
    df=pd.read_sql(select_sql,mssql_hook.get_sqlalchemy_engine())
    records=[]
    for _, row in df.iterrows():
        data=json.loads(row["raw_data"])
        hourly=data["hourly"]
        for i in range(len(hourly["time"])):
            records.append({
                "city":row["city"],
                "date":hourly["time"][i],
                "temp": hourly["temperature_2m"][i],
                "humidity": hourly["relative_humidity_2m"][i],
                "rain": hourly["rain"][i],
                "showers": hourly["showers"][i],
                "snowfall": hourly["snowfall"][i],
                "visibility": hourly["visibility"][i],
                "wind_speed": hourly["wind_speed_10m"][i],
                "weather_code": hourly["weather_code"][i],
                "ingested_at": row["ingested_at"]
            })
    df_silver = pd.DataFrame(records)
    if not df_silver.empty:
        df_silver["date"] = pd.to_datetime(df_silver["date"])
        df_silver = df_silver.drop_duplicates(subset=["city", "date"], keep="last")
    _delete_hourly_keys(mssql_hook, df_silver)
    _append_silver(mssql_hook, "Silver_weather_hourly_data", df_silver)
    print("✅تم نقل البيانات من جدول Bronze_weather_raw إلى جدول Silver_weather_hourly_data بنجاح.")

def create_silver_weather_current_table():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    create_table='''
          IF OBJECT_ID('Silver_weather_current_data','U') IS NULL
          BEGIN
               CREATE TABLE Silver_weather_current_data(
               id INT IDENTITY(1,1) PRIMARY KEY,
               city NVARCHAR(100),
               date DATETIME2,
               temperature FLOAT,
               is_day BIT,
               rain BIT,
               snowfall BIT,
               showers BIT,
               weather_code INT,
               wind_speed FLOAT,
               humidity FLOAT,
               precipitation BIT,
               ingested_at DATE
               )
          END


'''
    mssql_hook.run(create_table)
    print("✅تم إنشاء جدول Silver_weather_current_data بنجاح.")

def bronze_to_silver_weather_current():
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    select_sql='''
     SELECT  city, raw_data, ingested_at
     FROM Bronze_weather_raw
     WHERE ingested_at >= DATEADD(day, -1, GETDATE())
    '''
    df=pd.read_sql(select_sql,mssql_hook.get_sqlalchemy_engine())
    records=[]
    for _, row in df.iterrows():
        data=json.loads(row["raw_data"])
        current=data["current"]
        records.append({
            "city":row["city"],
            "date": current["time"],
            "temperature": current["temperature_2m"],
            "is_day": current["is_day"],
            "rain": current["rain"],
            "snowfall": current["snowfall"],
            "showers": current["showers"],
            "weather_code": current["weather_code"],
            "wind_speed": current["wind_speed_10m"],
            "humidity": current["relative_humidity_2m"],
            "precipitation": current["precipitation"],
            "ingested_at": row["ingested_at"]
        })
    
    df_silver = pd.DataFrame(records)
    if not df_silver.empty:
        df_silver["date"] = pd.to_datetime(df_silver["date"])
        df_silver = df_silver.drop_duplicates(subset=["city"], keep="last")
    _delete_current_keys(mssql_hook, df_silver)
    _append_silver(mssql_hook, "Silver_weather_current_data", df_silver)
    print("✅تم نقل البيانات من جدول Bronze_weather_raw إلى جدول Silver_weather_current_data بنجاح.")