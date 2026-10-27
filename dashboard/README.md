# Egypt Travel & Weather Dashboard

Interactive **Streamlit** dashboards for the weather × hospitality data pipeline.

## Quick Start

```bash
cd dashboard
python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Copy .env.example → .env and fill in your Snowflake credentials
cp .env.example .env

# Launch
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501)

## Dashboards

| Page | Description |
|------|-------------|
| 📊 **Tourism Index** | Composite city scores (0–100) with component breakdown — weather, landmarks, restaurants, hotel value |
| 🏨 **Hotel Analysis** | Price vs rating scatter, price tier distribution, value-for-money ranking, city comparisons |
| 🌡️ **Weather Dashboard** | Current temperatures, humidity, wind, and multi-day forecasts per city |
| 🍽️ **Dining & Landmarks** | Restaurant category distribution, landmark scores, city-level breakdowns |

## Architecture

```
Streamlit app  ──►  Snowflake (gold_layer + silver_layer)
                         ▲
                    dbt models
                         ▲
                    Airflow pipeline
```

## Data Freshness

The pipeline runs every 20 minutes. Dashboard queries are cached for 5 minutes (`@st.cache_data(ttl=300)`).
