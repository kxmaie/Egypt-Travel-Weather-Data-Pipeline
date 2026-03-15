from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
import requests
from datetime import datetime, timedelta
import pendulum
import pandas as pd
import json

def data_of_weather_api():
    url = "https://api.open-meteo.com/v1/forecast"
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_conn')
    
    
    create_table = '''
    IF OBJECT_ID('Bronze_weather_raw', 'U') IS NULL
    BEGIN
        CREATE TABLE Bronze_weather_raw (
            id INT IDENTITY(1,1) PRIMARY KEY,
            city NVARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            ingested_at DATETIME2,
            source NVARCHAR(100),
            raw_data NVARCHAR(MAX)
        );
    END
    '''
    mssql_hook.run(create_table)
    
    locations = [
        {"city": "القاهرة", "lat": 30.0444, "lon": 31.2357},
        {"city": "الجيزة", "lat": 29.9773, "lon": 31.1325},
        {"city": "الإسكندرية", "lat": 31.2001, "lon": 29.9187},
        {"city": "الأقصر", "lat": 25.6872, "lon": 32.6396}
    ]

    for loc in locations:
        city_name = loc["city"]
        
        
        check_sql = '''
        SELECT COUNT(1) AS cnt
        FROM Bronze_weather_raw
        WHERE city = %s
        AND CAST(ingested_at AS DATE) = CAST(GETDATE() AS DATE)
        '''
        
       
        result = mssql_hook.get_records(check_sql, parameters=(city_name))
        
        
        if result and result[0][0] > 0:
            print(f"⏭️ Data for {city_name} already ingested today. Skipping...")
            continue
        
        params = {
            "latitude": loc["lat"],
            "longitude": loc["lon"],
            "daily": "weather_code,temperature_2m_max,temperature_2m_min,rain_sum,showers_sum,snowfall_sum,wind_speed_10m_max,sunset,daylight_duration",
            "hourly": "temperature_2m,relative_humidity_2m,rain,showers,snowfall,weather_code,wind_speed_10m,visibility",
            "current": "temperature_2m,is_day,rain,snowfall,showers,weather_code,wind_speed_10m,relative_humidity_2m,precipitation",
            "timezone": "Africa/Cairo"
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            raw_data = response.text
            
            
            insert_sql = '''
            INSERT INTO Bronze_weather_raw (city, latitude, longitude, ingested_at, source, raw_data) 
            VALUES (%s, %s, %s, %s, %s, %s)
            '''
            
            
            mssql_hook.run(insert_sql, parameters=(
                city_name, 
                loc["lat"], 
                loc["lon"], 
                datetime.now(), 
                "open-meteo", 
                raw_data
            ))
               
            print(f"✅ تم تخزين بيانات {city_name} بنجاح.")
        else:
            print(f"❌ خطأ في جلب بيانات {city_name}: {response.status_code}")

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
    df_silver=pd.DataFrame(records)
    df_silver.to_sql("Silver_weather_daily_data", mssql_hook.get_sqlalchemy_engine(), if_exists="append", index=False)
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
    df_silver=pd.DataFrame(records)
    df_silver['date'] = pd.to_datetime(df_silver['date'])
    df_silver.to_sql("Silver_weather_hourly_data", mssql_hook.get_sqlalchemy_engine(), if_exists="append", index=False)
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
    
    df_silver=pd.DataFrame(records)
    df_silver["date"]=pd.to_datetime(df_silver["date"])
    df_silver.to_sql("Silver_weather_current_data", mssql_hook.get_sqlalchemy_engine(), if_exists="append", index=False)
    print("✅تم نقل البيانات من جدول Bronze_weather_raw إلى جدول Silver_weather_current_data بنجاح.")
def famous_landmark_data():
    url = "https://api.tomtom.com/search/2/search/Tourist Attraction.json"
    mssql_hook=MsSqlHook(mssql_conn_id='sql_server_conn')
    create_table = '''
    IF OBJECT_ID('Bronze_famous_landmark_data', 'U') IS NULL
    BEGIN
        CREATE TABLE Bronze_famous_landmark_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            city NVARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            ingested_at DATETIME2,
            source NVARCHAR(100),
            raw_data NVARCHAR(MAX)
        );
    END
    '''
    mssql_hook.run(create_table)

    locations = [
        {"city": "القاهرة", "lat": 30.0444, "lon": 31.2357},
        {"city": "الجيزة", "lat": 29.9773, "lon": 31.1325},
        {"city": "الإسكندرية", "lat": 31.2001, "lon": 29.9187},
        {"city": "الأقصر", "lat": 25.6872, "lon": 32.6396}
    ]

    
    for loc in locations:
        city_name = loc["city"]
        
      
        check_sql = '''
        SELECT COUNT(1) FROM Bronze_famous_landmark_data 
        WHERE city = %s AND CAST(ingested_at AS DATE) = CAST(GETDATE() AS DATE)
        '''
        
        
        already_exists = mssql_hook.get_records(check_sql, parameters=(city_name))
            
        if already_exists[0][0] > 0:
                print(f"⏭️ البيانات الخاصة بـ {city_name} موجودة بالفعل اليوم. تخطي...")
                continue 

            
        params = {
                "key": "4Su8KBv5JREPEl8CmlXhJU2sPnwbVxmS",
                "lat": loc["lat"],
                "lon": loc["lon"],
                "radius": 40000,
                "language": "ar",
                "view": "Unified"
            }
            
        response = requests.get(url, params=params)
            
        if response.status_code == 200:
                raw_data = response.text
                insert_sql = '''
                INSERT INTO Bronze_famous_landmark_data (city, latitude, longitude, ingested_at, source, raw_data)
                VALUES (%s,%s,%s,%s,%s,%s)
                '''

                mssql_hook.run(insert_sql, parameters=(
                    city_name,
                    loc["lat"],
                    loc["lon"],
                    datetime.now(),
                    "TomTom API",
                    raw_data
                    ))
                print(f"✅ تم تخزين بيانات {city_name} بنجاح.")
        else:
                print(f"❌ خطأ في جلب بيانات {city_name}: {response.status_code}")

def create_famous_landmark_table():
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_conn')
    create_table = '''
    IF OBJECT_ID('Silver_famous_landmark_data', 'U') IS NULL
    BEGIN
        CREATE TABLE Silver_famous_landmark_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            city NVARCHAR(100),
            score FLOAT,
            distance FLOAT,
            latitude FLOAT,
            longitude FLOAT,
            address NVARCHAR(255),
            category NVARCHAR(100),
            landmark_name NVARCHAR(100),
            ingested_at DATETIME2
            
        );
    END
    '''
    mssql_hook.run(create_table)
    print("✅تم إنشاء جدول Silver_famous_landmark_data بنجاح.")

def bronze_to_silver_famous_landmark():
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_conn')
    select_sql='''SELECT city, raw_data, ingested_at
                   FROM Bronze_famous_landmark_data
                   WHERE ingested_at >= DATEADD(day, -1, GETDATE())
                '''
    df=pd.read_sql(select_sql, mssql_hook.get_sqlalchemy_engine())
    records=[]
    for _, row in df.iterrows():
        data=json.loads(row["raw_data"])
        result=data["results"]
        for places in result:
            name=places["poi"]["name"]
            category=places["poi"]["categories"][0] if places["poi"].get("categories") else None
            address = places["address"].get("freeformAddress")
            lat = places["position"]["lat"]
            lon = places["position"]["lon"]
            dist = places.get("dist")
            score = places.get("score")
            records.append({
            "city": row["city"],
            "landmark_name": name,
            "score": score,
            "category": category,
            "address": address,
            "latitude": lat,
            "longitude": lon,
            "distance": dist,
            "ingested_at": row["ingested_at"]
        })
    df_silver=pd.DataFrame(records)
    df_silver.to_sql("Silver_famous_landmark_data", mssql_hook.get_sqlalchemy_engine(), if_exists="append", index=False)
    print(" ✅تم نقل بيانات المعالم السياحية من جدول Bronze إلى جدول Silver بنجاح.")

def load_resturants_csv_files_to_sql(file_path):
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_conn')
    create_table = '''
    IF OBJECT_ID('Bronze_restaurant_data', 'U') IS NULL
    BEGIN
        CREATE TABLE Bronze_restaurant_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            resturantid INT,
            Restaurant_Name NVARCHAR(100),
            Category NVARCHAR(255),
            City NVARCHAR(100),
            Area NVARCHAR(100),
            Longitude FLOAT,
            Latitude FLOAT,
            Ingested_At DATETIME2
        );
    END
    '''
    mssql_hook.run(create_table)

    df=pd.read_csv(file_path,encoding="utf-8")
    df=df.rename(columns={
        "Restaurant ID": "resturantid",
        "Restaurant Name": "Restaurant_Name",
        "Category": "Category",
        "City": "City",
        "Area": "Area",
        "Longitude": "Longitude",
        "Latitude": "Latitude"

    })
    df["Ingested_At"]=datetime.now()
    df.to_sql("Bronze_restaurant_data", mssql_hook.get_sqlalchemy_engine(), if_exists="append", index=False)
    print(" ✅تم نقل بيانات المطاعم من جدول Bronze إلى جدول Silver بنجاح.")

def load_hotels_csv_to_sql(file_path):
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_conn')
    create_table = '''
    IF OBJECT_ID('Bronze_hotel_data', 'U') IS NULL
    BEGIN
        CREATE TABLE Bronze_hotel_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            type NVARCHAR(100),
            name NVARCHAR(250),
            link NVARCHAR(max),
            latitude FLOAT,
            longitude FLOAT,
            check_in_time NVARCHAR(100),
            check_out_time NVARCHAR(100),
            overall_rating FLOAT,
            reviews NVARCHAR(max),
            location_rating FLOAT,
            amenities NVARCHAR(max),
            excluded_amenities NVARCHAR(max),
            essential_info NVARCHAR(max),
            nearby_places NVARCHAR(max),
            ratings_breakdown NVARCHAR(max),
            reviews_breakdown NVARCHAR(max),
            rate_per_night_lowest NVARCHAR(150),
            rate_per_night_before_taxes_and_fees NVARCHAR(150),
            total_rate_lowest NVARCHAR(150),
            total_rate_before_taxes_and_fees NVARCHAR(150),
            ingested_at DATETIME2
        );
    END
    '''
    mssql_hook.run(create_table)
    df=pd.read_csv(file_path,encoding="utf-8")
    df=df.rename(columns={
        "Type":"type",
        "Name":"name",
        "Link":"link",
        "Latitude":"latitude",
        "Longitude":"longitude",
        "Check-In Time":"check_in_time",
        "Check-Out Time":"check_out_time",
        "Overall Rating":"overall_rating",
        "Reviews":"reviews",
        "Location Rating":"location_rating",
        "Amenities":"amenities",
        "Excluded Amenities":"excluded_amenities",
        "Essential Info":"essential_info",
        "Nearby Places":"nearby_places",
        "Ratings Breakdown":"ratings_breakdown",
        "Reviews Breakdown":"reviews_breakdown",
        "Rate per Night (Lowest)":"rate_per_night_lowest",
        "Rate per Night (Before Taxes and Fees)":"rate_per_night_before_taxes_and_fees",
        "Total Rate (Lowest)":"total_rate_lowest",
        "Total Rate (Before Taxes and Fees)":"total_rate_before_taxes_and_fees"
       
    })
    df["ingested_at"]=datetime.now()
    df.to_sql("Bronze_hotel_data", mssql_hook.get_sqlalchemy_engine(), if_exists="append", index=False)
    print(" ✅تم نقل بيانات الفنادق من جدول Bronze إلى جدول Silver بنجاح.")

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

local_tz = pendulum.timezone("Africa/Cairo")
default_args = {
    "owner": "momen",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 3, 4, tz=local_tz),
    "retries": 1, 
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="rental_and_weather",
    description="A DAG to ingest weather data and compare it with rental data",
    schedule="0 0 * * *",
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=30)
) as dag:
    fetch_weather_data_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=data_of_weather_api
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

    famous_landmark_data_task=PythonOperator(
        task_id="famous_landmark_data",
        python_callable=famous_landmark_data
    )

    create_famous_landmark_table_task=PythonOperator(
        task_id="create_famous_landmark_table",
        python_callable=create_famous_landmark_table
    )

    bronze_to_silver_famous_landmark_task=PythonOperator(
        task_id="bronze_to_silver_famous_landmark",
        python_callable=bronze_to_silver_famous_landmark
    )
    
    load_resturants_csv_files_to_sql_task=PythonOperator(
        task_id="load_resturants_csv_files_to_sql",
        python_callable=load_resturants_csv_files_to_sql,
        op_kwargs={"file_path": "/usr/local/airflow/include/Egyptian_restaurants.csv"}
    )

    load_hotels_csv_to_sql_task=PythonOperator(
        task_id="load_hotels_csv_to_sql",
        python_callable=load_hotels_csv_to_sql,
        op_kwargs={"file_path": "/usr/local/airflow/include/Egypt_hotels_data.csv"}
    )
    
    load_hotel_table_to_snowflake_task=PythonOperator(
        task_id="load_hotel_table_to_snowflake",
        python_callable=transfer_table_hotels_to_snowflake
    )

    load_resturants_table_to_snowflake_task=PythonOperator(
        task_id="load_resturants_table_to_snowflake",
        python_callable=transfer_tables_resturant_to_snowflake
    )


    load_famous_landmark_table_to_snowflake_task=PythonOperator(
        task_id="load_famous_landmark_table_to_snowflake",
        python_callable=transfer_table_famous_landmark_to_snowflake
    )

    load_current_weather_table_to_snowflake_task=PythonOperator(
        task_id="load_current_weather_table_to_snowflake",
        python_callable=transfer_table_current_weather_to_snowflake
    )


    load_daily_weather_table_to_snowflake_task=PythonOperator(
        task_id="load_daily_weather_table_to_snowflake",
        python_callable=transfer_table_daily_weather_to_snowflake
    )

    load_hourly_weather_table_to_snowflake_task=PythonOperator(
        task_id="load_hourly_weather_table_to_snowflake",
        python_callable=transfer_table_hourly_weather_to_snowflake
    )


fetch_weather_data_task >> [ create_silver_daily_weather_table_task,
    create_silver_weather_hourly_table_task,
    create_silver_weather_current_table_task]

create_silver_daily_weather_table_task >> bronze_to_silver_daily_weather_task
create_silver_weather_hourly_table_task >> bronze_to_silver_weather_hourly_task
create_silver_weather_current_table_task >> bronze_to_silver_weather_current_task
create_famous_landmark_table_task >> famous_landmark_data_task >> bronze_to_silver_famous_landmark_task
load_resturants_csv_files_to_sql_task >> load_hotels_csv_to_sql_task >> [load_hotel_table_to_snowflake_task,
                                                                        load_resturants_table_to_snowflake_task,
                                                                        load_famous_landmark_table_to_snowflake_task,
                                                                        load_current_weather_table_to_snowflake_task,
                                                                        load_daily_weather_table_to_snowflake_task,
                                                                        load_hourly_weather_table_to_snowflake_task]