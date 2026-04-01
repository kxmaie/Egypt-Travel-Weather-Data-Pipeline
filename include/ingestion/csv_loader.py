import pandas as pd
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
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
    mssql_hook.run(
        """
        IF OBJECT_ID('Bronze_restaurant_data', 'U') IS NOT NULL
            TRUNCATE TABLE Bronze_restaurant_data;
        """
    )

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
    mssql_hook.run(
        """
        IF OBJECT_ID('Bronze_hotel_data', 'U') IS NOT NULL
            TRUNCATE TABLE Bronze_hotel_data;
        """
    )
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