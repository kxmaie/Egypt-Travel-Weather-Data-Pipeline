# CI/CD Workflows

Three validation workflows run automatically on pushes / pull requests to `master` or `main`. The Snowflake integration workflow is manual and uses an isolated CI database and schema prefix.

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| **Airflow DAG Validation** | changes in `dags/` or `include/` | Validates DAG imports, checks tags + retries ≥ 2, runs pytest |
| **dbt CI** | changes in `include/dbt_project/` | Resolves dbt packages and parses the project without warehouse credentials |
| **dbt Snowflake Integration** | manual dispatch | Runs `dbt build` against CI-only schemas and removes those schemas afterward |
| **Python Lint** | changes in `*.py` | Ruff lint + format check on dags, include, and dashboard code |

## Required GitHub Secrets (for manual Snowflake integration)

Add these under **Settings → Secrets and variables → Actions**:

| Secret | Example |
|--------|---------|
| `SNOWFLAKE_CI_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_CI_DATABASE` | Database where the CI role can create isolated schemas |
| `SNOWFLAKE_CI_PASSWORD` | CI-only user password |
| `SNOWFLAKE_CI_ROLE` | Least-privilege CI role |
| `SNOWFLAKE_CI_USER` | CI-only Snowflake user |
| `SNOWFLAKE_CI_WAREHOUSE` | CI warehouse |
| `SNOWFLAKE_SOURCE_DATABASE` | Database with read-only access to the loaded source tables |

## Workflow Diagram

```
Push / PR to master
       │
       ├─► Airflow DAG Validation
       │     ├─ Install Airflow + providers
       │     ├─ DagBag import check (tags, retries)
       │     └─ pytest tests/dags/
       │
       ├─► dbt CI (no warehouse secrets)
       │     ├─ Install dbt-snowflake
       │     └─ dbt deps + dbt parse
       │
       └─► Python Lint
             └─ ruff check + format

Manual **dbt Snowflake Integration** runs `dbt seed` and `dbt build` using the CI-only role, prefixes output schemas with a unique run ID, then drops those isolated schemas. The CI role needs read access to the source database and create/drop access only in the CI target database.
```
