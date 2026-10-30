import logging
import json
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

log = logging.getLogger(__name__)


def _delete_landmark_batch(mssql_hook, df: pd.DataFrame) -> None:
    if df.empty:
        return
    cities = df["city"].unique().tolist()
    ph = ",".join(["%s"] * len(cities))
    sql = (
        f"DELETE FROM Silver_famous_landmark_data WHERE city IN ({ph}) "
        "AND ingested_at >= DATEADD(day, -1, GETDATE())"
    )
    mssql_hook.run(sql, parameters=tuple(cities))


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
    log.info("Table Silver_famous_landmark_data is ready.")


def bronze_to_silver_famous_landmark():
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_conn')
    select_sql = '''
    SELECT city, raw_data, ingested_at
    FROM Bronze_famous_landmark_data
    WHERE ingested_at >= DATEADD(day, -1, GETDATE())
    '''
    df = pd.read_sql(select_sql, mssql_hook.get_sqlalchemy_engine())
    records = []
    for _, row in df.iterrows():
        data = json.loads(row["raw_data"])
        result = data["results"]
        for place in result:
            name = place["poi"]["name"]
            category = place["poi"]["categories"][0] if place["poi"].get("categories") else None
            address = place["address"].get("freeformAddress")
            lat = place["position"]["lat"]
            lon = place["position"]["lon"]
            dist = place.get("dist")
            score = place.get("score")
            records.append({
                "city": row["city"],
                "landmark_name": name,
                "score": score,
                "category": category,
                "address": address,
                "latitude": lat,
                "longitude": lon,
                "distance": dist,
                "ingested_at": row["ingested_at"],
            })
    df_silver = pd.DataFrame(records)
    if not df_silver.empty:
        df_silver = df_silver.drop_duplicates(subset=["city", "landmark_name"], keep="last")
    _delete_landmark_batch(mssql_hook, df_silver)
    if not df_silver.empty:
        df_silver.to_sql(
            "Silver_famous_landmark_data",
            mssql_hook.get_sqlalchemy_engine(),
            if_exists="append",
            index=False,
        )
    log.info(
        "Moved %d landmark rows from Bronze_famous_landmark_data to Silver_famous_landmark_data.",
        len(df_silver),
    )
