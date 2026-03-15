import requests
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

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