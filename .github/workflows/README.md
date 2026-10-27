# CI/CD Workflows

Three GitHub Actions workflows run automatically on every push / PR to `master`:

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| **Airflow DAG Validation** | changes in `dags/` or `include/` | Validates DAG imports, checks tags + retries ≥ 2, runs pytest |
| **dbt CI** | changes in `include/dbt_project/` | Runs `dbt seed → dbt run → dbt test` against Snowflake, uploads artifacts |
| **Python Lint** | changes in `*.py` | Ruff lint + format check on dags, include, and dashboard code |

## Required GitHub Secrets (for dbt CI)

Add these under **Settings → Secrets and variables → Actions**:

| Secret | Example |
|--------|---------|
| `SNOWFLAKE_ACCOUNT` | `RO04574.eu-central-2.aws` |
| `SNOWFLAKE_DATABASE` | `WEATHER_DB` |
| `SNOWFLAKE_PASSWORD` | `your_password` |
| `SNOWFLAKE_ROLE` | `WEATHER_PROJECT_ROLE` |
| `SNOWFLAKE_SCHEMA` | `RAW_DATA` |
| `SNOWFLAKE_USER` | `AIRFLOW_USER` |
| `SNOWFLAKE_WAREHOUSE` | `WEATHER_WH` |

## Workflow Diagram

```
Push / PR to master
       │
       ├─► Airflow DAG Validation
       │     ├─ Install Airflow + providers
       │     ├─ DagBag import check (tags, retries)
       │     └─ pytest tests/dags/
       │
       ├─► dbt CI
       │     ├─ Install dbt-snowflake
       │     ├─ dbt seed (city_name_mapping)
       │     ├─ dbt run (staging + marts)
       │     ├─ dbt test (not_null, unique, singular)
       │     └─ Upload artifacts (manifest, docs)
       │
       └─► Python Lint
             └─ ruff check + format
```
