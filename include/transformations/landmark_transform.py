import pandas as pd
import json
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
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