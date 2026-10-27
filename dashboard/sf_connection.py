"""
Snowflake connection helper for the Streamlit dashboard.

Reads credentials from environment variables / .env file.
"""

import os
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


@st.cache_resource
def get_snowflake_connection():
    """Return a cached Snowflake connection for the dashboard session."""
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "WEATHER_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "WEATHER_DB"),
        role=os.getenv("SNOWFLAKE_ROLE", "WEATHER_PROJECT_ROLE"),
    )
    return conn


@st.cache_data(ttl=300)
def run_query(sql: str) -> "list[tuple]":
    """Run a SQL query and return rows (cached for 5 min)."""
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return rows


def get_schema(schema_env: str = "SNOWFLAKE_GOLD_SCHEMA") -> str:
    return os.getenv(schema_env, "gold_layer")