# Weather & Hospitality Data Pipeline

End-to-end ELT built on **Astronomer Airflow**: ingest APIs and CSV files into **Microsoft SQL Server** (bronze/silver), load into **Snowflake**, then model with **dbt** (staging in `silver_layer`, marts in `gold_layer`).

## Architecture

```text
Open-Meteo (weather) ──┐
TomTom (landmarks)  ───┼──► SQL Server (Bronze → Silver) ──► Snowflake RAW_DATA ──► dbt staging ──► dbt marts (gold)
CSV (hotels / restaurants) ─┘
```

Airflow DAGs (scheduled every 20 minutes):

1. **`ingestion_dag`** — weather, landmarks, restaurants CSV, hotels CSV
2. **`transformation_dag`** — builds silver tables from bronze (Python + pandas), after `ExternalTaskSensor` on ingestion
3. **`loader_dag`** — copies silver/bronze tables to Snowflake with `write_pandas`, after sensor on transformation

`ExternalTaskSensor` uses `execution_delta` to align logical dates between DAGs; tune it if you change schedules.

## Repository layout

| Path | Role |
|------|------|
| `dags/` | Airflow DAG definitions (3 DAGs) |
| `include/ingestion/` | API + CSV load into SQL Server (bronze) |
| `include/transformations/` | Bronze → silver transforms (weather + landmarks) |
| `include/loaders/` | SQL Server → Snowflake loader (DRY helper) |
| `include/dbt_project/` | dbt project: staging, marts, tests, seeds |
| `include/*.csv` | Sample static files (paths in DAGs assume Astro `/usr/local/airflow/include/`) |
| `INSIGHTS_DASHBOARD.md` | Comprehensive insights & dashboard SQL guide |

## dbt Models

### Staging (silver_layer)
- `stg_daily_weather_data` — daily forecast (deduped by city + date)
- `stg_hourly_weather_data` — hourly weather (deduped by city + timestamp)
- `stg_current_weather_data` — current conditions (one row per city)
- `stg_hotel_data` — hotels with price/rating extraction (deduped by name + coords)
- `stg_restaurant_data` — restaurants (deduped by source id + city)
- `stg_famous_landmark_data` — landmarks with rank within category

### Marts (gold_layer)
- `mart_city_destination_profile` — city-level weather + landmark + restaurant profile (uses city_name_mapping seed)
- `mart_hotel_analysis` — hotel price tiers, value-for-money scores, amenity counts, weather context
- `mart_city_tourism_index` — composite tourism score (0–100) per city with tier label

### Seeds
- `city_name_mapping` — maps Arabic ↔ English city names for cross-source joins

### Tests
- `models/staging/staging.yml` — not_null tests on key columns
- `models/marts/marts.yml` — not_null + unique on mart grains
- `tests/staging/` — singular SQL dedup assertions (5 tables)
- `tests/staging/assert_no_duplicate_hotel_id.sql` — dedup on hotel mart

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview/) for local Airflow
- Reachable **SQL Server** and **Snowflake** accounts
- **dbt** with Snowflake adapter

## Airflow connections

Configure in the Airflow UI or `airflow_settings.yaml` (local):

| Connection ID | Type | Purpose |
|---------------|------|---------|
| `sql_server_conn` | Microsoft SQL Server | Bronze/silver tables |
| `conn_snowflake` | Snowflake | `RENTAL_WEATHER_DB.RAW_DATA` loads |

**TomTom API key**: set Airflow Variable `TOMTOM_API_KEY` or environment variable `TOMTOM_API_KEY`. Never commit real keys.

## Environment variables (for dbt)

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_DATABASE` | Target database (default: WEATHER_DB) |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_ROLE` | Role (default: WEATHER_PROJECT_ROLE) |
| `SNOWFLAKE_SCHEMA` | Schema (default: RAW_DATA) |
| `SNOWFLAKE_USER` | User (default: AIRFLOW_USER) |
| `SNOWFLAKE_WAREHOUSE` | Warehouse (default: WEATHER_WH) |
| `TOMTOM_API_KEY` | TomTom Search API key |

## Local Airflow

```bash
astro dev start
```

UI: [http://localhost:8080](http://localhost:8080)

## dbt

From the dbt project directory:

```bash
cd include/dbt_project
dbt seed          # loads city_name_mapping.csv
dbt run           # staging (silver_layer) + marts (gold_layer)
dbt test          # staging + mart tests + singular dedup tests
```

- **Profile**: `profiles.yml` uses `env_var()` for all secrets — never commit real credentials.
- **Sources**: `models/sources.yml` → Snowflake `RENTAL_WEATHER_DB.RAW_DATA`.

## Insights & Dashboards

### SQL-based insights
See [`INSIGHTS_DASHBOARD.md`](INSIGHTS_DASHBOARD.md) for:
- Architecture diagram & data relationships
- City tourism index methodology
- Hotel analysis SQL queries
- Weather × tourism cross-analysis
- Restaurant & landmark breakdowns
- Key insights and data gaps

### Interactive Streamlit Dashboard

The [`dashboard/`](dashboard/) folder contains a full **Streamlit** app with 4 interactive pages:

| Page | Description |
|------|-------------|
| 📊 Tourism Index | Composite city scores (0–100) with component breakdown |
| 🏨 Hotel Analysis | Price vs rating, price tiers, value-for-money ranking |
| 🌡️ Weather Dashboard | Current conditions + multi-day forecasts |
| 🍽️ Dining & Landmarks | Restaurant categories + landmark scores |

```bash
cd dashboard
pip install -r requirements.txt
cp .env.example .env   # fill in Snowflake credentials
streamlit run app.py
```

## CI/CD (GitHub Actions)

Three workflows run on every push / PR:

| Workflow | Purpose |
|----------|---------|
| **Airflow DAG Validation** | DAG imports, tags, retries ≥ 2, pytest |
| **dbt CI** | `dbt seed → run → test` against Snowflake + artifact upload |
| **Python Lint** | Ruff lint + format on dags, include, dashboard |

See [`.github/workflows/README.md`](.github/workflows/README.md) for setup instructions and required secrets.

## DAG tests (pytest)

`tests/dags/test_dag_example.py` expects DAG `tags` and `retries >= 2`. Project DAGs are aligned (`retries: 2`, `tags` on each DAG).

## License / contact

Update this section for your own use case.
