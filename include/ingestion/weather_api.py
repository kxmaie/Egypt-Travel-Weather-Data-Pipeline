import requests
import json
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

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