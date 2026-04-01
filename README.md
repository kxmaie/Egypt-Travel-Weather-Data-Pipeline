# Weather & hospitality data pipeline

End-to-end ELT built on **Astronomer Airflow**: ingest APIs and CSV files into **Microsoft SQL Server** (bronze/silver), load into **Snowflake**, then model with **dbt** (staging in `silver_layer`).

## Architecture

```text
Open-Meteo (weather) ──┐
TomTom (landmarks)  ───┼──► SQL Server (Bronze → Silver) ──► Snowflake RAW_DATA ──► dbt staging ──► dbt marts (gold)
CSV (hotels / restaurants) ─┘
```

Airflow DAGs (scheduled every 20 minutes):

1. **`ingestion_dag`** — weather, landmarks, restaurants CSV, hotels CSV  
2. **`transformation_dag`** — builds silver tables from bronze (Python + pandas), after `ExternalTaskSensor` on ingestion  
3. **`loader_dag`** — copies tables to Snowflake with `write_pandas`, after sensor on transformation  

`ExternalTaskSensor` uses `execution_delta` to align logical dates between DAGs; tune it if you change schedules.

## Repository layout

| Path | Role |
|------|------|
| `dags/` | Airflow DAG definitions |
| `include/ingestion/` | API + CSV load into SQL Server |
| `include/transformations/` | Bronze → silver transforms |
| `include/loaders/` | SQL Server → Snowflake |
| `include/dbt_project/` | dbt: `models/staging` (silver), `models/marts` (gold), tests |
| `include/*.csv` | Sample static files (paths in DAGs assume Astro `/usr/local/airflow/include/`) |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview/) for local Airflow  
- Reachable **SQL Server** and **Snowflake** accounts  
- **dbt** with Snowflake adapter (for running models outside Astro if you want)

## Airflow connections

Configure in the Airflow UI or `airflow_settings.yaml` (local):

| Connection ID | Type | Purpose |
|---------------|------|---------|
| `sql_server_conn` | Microsoft SQL Server | Bronze/silver tables |
| `conn_snowflake` | Snowflake | `RENTAL_WEATHER_DB.RAW_DATA` loads |

**TomTom API key** (landmarks task): set Airflow Variable `TOMTOM_API_KEY` or environment variable `TOMTOM_API_KEY`. For local Astro, use `airflow_settings.yaml` under `variables` (do not commit real keys; keep `variable_value` empty in git and paste locally).

## Local Airflow

```bash
astro dev start
```

UI: [http://localhost:8080](http://localhost:8080) (default credentials from Astro docs).

## dbt

From the dbt project directory:

```bash
cd include/dbt_project
dbt deps          # if you add packages later
dbt run           # staging (silver_layer) + marts (gold_layer)
dbt test          # staging + mart tests + duplicate-grain singular tests
```

- **Profile**: `profiles.yml` (use env vars or `env_var()` for passwords; do not commit secrets).  
- **Sources**: `models/sources.yml` → Snowflake `RENTAL_WEATHER_DB.RAW_DATA`.  
- **Staging**: weather (current/daily/hourly), hotels, landmarks, restaurants — see `models/staging/`.  
- **Marts**: `mart_city_destination_profile` joins latest daily + current weather with landmark/restaurant counts per city (`models/marts/`). City names are not normalized across English/Arabic sources; extend with a mapping seed if you need strict joins.  
- **Tests**: `models/staging/staging.yml`, `models/marts/marts.yml`, and **singular** SQL tests under `tests/staging/` (dedupe grains).

Run `dbt test` after `dbt run` and after the Airflow loader has refreshed Snowflake so tables are non-empty (otherwise some `not_null` tests may not reflect real issues).

## Python dependencies

See `requirements.txt` (e.g. MSSQL and Snowflake Airflow providers). Astro Runtime includes many providers; add any missing ones per [Astro docs](https://www.astronomer.io/docs/astro/runtime-image-architecture).

## DAG tests (pytest)

`tests/dags/test_dag_example.py` expects DAG `tags` and `retries >= 2`. Project DAGs are aligned (`retries: 2`, `tags` on each DAG). Run from repo root with `AIRFLOW_HOME` set appropriately for your environment (see Astro docs).

## License / contact

Update this section for your own use case.
