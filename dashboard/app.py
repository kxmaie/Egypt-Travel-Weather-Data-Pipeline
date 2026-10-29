"""
Egypt Travel & Weather — Interactive Streamlit Dashboard

Run:  streamlit run dashboard/app.py
"""

import os
import sys
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Make the dashboard folder importable when running from project root
sys.path.insert(0, os.path.dirname(__file__))
from sf_connection import run_query, get_schema

# ─────────────────────────── Page config ────────────────────────────────────

st.set_page_config(
    page_title="Egypt Travel & Weather Dashboard",
    page_icon="🏜️",
    layout="wide",
    initial_sidebar_state="expanded",
)

GOLD = get_schema("SNOWFLAKE_GOLD_SCHEMA")
SILVER = get_schema("SNOWFLAKE_SILVER_SCHEMA")


# ─────────────────────────── Sidebar ─────────────────────────────────────────

st.sidebar.title("🏜️ Egypt Travel & Weather")
st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Navigate",
    ["📊 Tourism Index", "🏨 Hotel Analysis", "🌡️ Weather Dashboard", "🍽️ Dining & Landmarks"],
)
st.sidebar.markdown("---")
st.sidebar.caption("Data from Airflow → SQL Server → Snowflake → dbt")
st.sidebar.caption("Pipeline runs daily")


# ─────────────────────────── Page 1: Tourism Index ─────────────────────────

def page_tourism_index():
    st.title("📊 City Tourism Index")
    st.markdown("Composite score per city — combining weather, landmarks, restaurants, and hotel value.")

    # Load data
    rows = run_query(f"""
        SELECT city, daily_max_temp_c, daily_min_temp_c, current_temp_c,
               current_humidity_pct, landmark_count, restaurant_count, restaurant_variety,
               hotel_count, avg_hotel_price_per_night, avg_hotel_score,
               weather_comfort_score, wind_score, landmark_score,
               restaurant_score, hotel_value_score, tourism_index,
               data_coverage_pct, index_methodology_version, destination_tier
        FROM {GOLD}.mart_city_tourism_index
        ORDER BY tourism_index DESC NULLS LAST
    """)

    if not rows:
        st.warning("No data yet. Run the Airflow pipeline + `dbt run` first.")
        return

    cols = [
        "city", "daily_max_temp_c", "daily_min_temp_c", "current_temp_c",
        "current_humidity_pct", "landmark_count", "restaurant_count", "restaurant_variety",
        "hotel_count", "avg_hotel_price_per_night", "avg_hotel_score",
        "weather_comfort_score", "wind_score", "landmark_score",
        "restaurant_score", "hotel_value_score", "tourism_index",
        "data_coverage_pct", "index_methodology_version", "destination_tier",
    ]
    df = pd.DataFrame(rows, columns=cols)

    # --- Top KPIs ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cities Analyzed", len(df))
    c2.metric("Top Destination", df.iloc[0]["city"] if len(df) else "—")
    c3.metric("Highest Index", f"{df['tourism_index'].max():.1f}" if len(df) else "—")
    c4.metric("Total Landmarks", int(df["landmark_count"].sum()))

    st.markdown("---")

    # --- Tourism index bar chart ---
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["city"],
        y=df["tourism_index"],
        text=df["tourism_index"].round(1),
        textposition="auto",
        marker_color=df["tourism_index"],
        marker_colorscale="Viridis",
        hovertemplate="<b>%{x}</b><br>Index: %{y:.1f}<extra></extra>",
    ))
    fig.update_layout(
        title="Tourism Index by City (0–100)",
        xaxis_title="City",
        yaxis_title="Tourism Index",
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Component scores (stacked bar) ---
    comp_cols = ["weather_comfort_score", "landmark_score", "restaurant_score", "hotel_value_score"]
    comp_labels = ["Weather Comfort", "Landmarks", "Restaurants", "Hotel Value"]
    colors = ["#3498db", "#e74c3c", "#2ecc71", "#f39c12"]

    fig2 = go.Figure()
    for col, label, color in zip(comp_cols, comp_labels, colors):
        fig2.add_trace(go.Bar(
            name=label, x=df["city"], y=df[col],
            marker_color=color, hovertemplate=f"<b>{label}</b>: %{{y:.1f}}<extra></extra>",
        ))
    fig2.update_layout(
        title="Score Components by City",
        barmode="group",
        xaxis_title="City",
        yaxis_title="Score (0–100)",
        height=400,
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.caption(
        "Scores renormalize weights over available data. Coverage shows the share "
        "of the index weight supported by data; restaurant variety counts distinct "
        "categories. Relative components compare cities "
        "within this build."
    )

    # --- Data table ---
    with st.expander("📋 Detailed Table"):
        st.dataframe(df.style.format({
            "daily_max_temp_c": "{:.1f}",
            "daily_min_temp_c": "{:.1f}",
            "current_temp_c": "{:.1f}",
            "current_humidity_pct": "{:.1f}",
            "avg_hotel_price_per_night": "{:.0f}",
            "avg_hotel_score": "{:.2f}",
            "weather_comfort_score": "{:.1f}",
            "wind_score": "{:.1f}",
            "landmark_score": "{:.1f}",
            "restaurant_score": "{:.1f}",
            "hotel_value_score": "{:.1f}",
            "tourism_index": "{:.1f}",
            "data_coverage_pct": "{:.1f}%",
        }), use_container_width=True)


# ─────────────────────────── Page 2: Hotel Analysis ─────────────────────────

def page_hotel_analysis():
    st.title("🏨 Hotel Analysis")
    st.markdown("Price tiers, value-for-money scores, and weather context per hotel.")

    rows = run_query(f"""
        SELECT hotel_id, hotel_name, type, nearest_city, approx_distance_km,
               final_price, base_price, total_taxes, weighted_score,
               overall_rating, location_rating, recommended_rank,
               value_score, tax_pct, price_tier, amenity_count,
               weather_max_temp_c, weather_min_temp_c
        FROM {GOLD}.mart_hotel_analysis
        WHERE final_price > 0
        ORDER BY value_score DESC
    """)

    if not rows:
        st.warning("No hotel data yet. Run the pipeline + `dbt run` first.")
        return

    cols = [
        "hotel_id", "hotel_name", "type", "nearest_city", "approx_distance_km",
        "final_price", "base_price", "total_taxes", "weighted_score",
        "overall_rating", "location_rating", "recommended_rank",
        "value_score", "tax_pct", "price_tier", "amenity_count",
        "weather_max_temp_c", "weather_min_temp_c",
    ]
    df = pd.DataFrame(rows, columns=cols)

    # --- Filters ---
    c1, c2 = st.columns(2)
    cities = st.multiselect("Filter by City", sorted(df["nearest_city"].dropna().unique()), default=None)
    tiers = st.multiselect("Filter by Price Tier", sorted(df["price_tier"].dropna().unique()), default=None)

    if cities:
        df = df[df["nearest_city"].isin(cities)]
    if tiers:
        df = df[df["price_tier"].isin(tiers)]

    # --- KPIs ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Hotels", len(df))
    c2.metric("Avg Price/Night", f"{df['final_price'].mean():.0f} EGP" if len(df) else "—")
    c3.metric("Avg Rating", f"{df['overall_rating'].mean():.1f}" if len(df) and df['overall_rating'].sum() > 0 else "—")
    c4.metric("Top Value Hotel", df.iloc[0]["hotel_name"] if len(df) else "—")

    st.markdown("---")

    # --- Scatter: Price vs Rating (sized by value_score, colored by tier) ---
    fig = px.scatter(
        df, x="final_price", y="weighted_score",
        size="amenity_count", color="price_tier",
        hover_name="hotel_name",
        hover_data={"nearest_city": True, "value_score": ":.4f", "overall_rating": True},
        title="Price vs Weighted Score (bubble size = amenities, color = price tier)",
        labels={"final_price": "Price per Night (EGP)", "weighted_score": "Weighted Score"},
        height=500,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Price tier distribution ---
    col1, col2 = st.columns(2)
    with col1:
        tier_df = df["price_tier"].value_counts().reset_index()
        tier_df.columns = ["tier", "count"]
        fig2 = px.pie(tier_df, values="count", names="tier", title="Price Tier Distribution", height=350)
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        city_price = df.groupby("nearest_city")["final_price"].mean().reset_index()
        city_price = city_price.sort_values("final_price", ascending=True)
        fig3 = px.bar(
            city_price, x="final_price", y="nearest_city", orientation="h",
            title="Avg Price per Night by City", height=350,
            labels={"final_price": "Avg Price (EGP)", "nearest_city": "City"},
        )
        st.plotly_chart(fig3, use_container_width=True)

    # --- Data table ---
    with st.expander("📋 Top 20 Hotels by Value Score"):
        display_df = df.head(20)[["hotel_name", "nearest_city", "type", "final_price", "overall_rating", "value_score", "price_tier", "amenity_count"]]
        st.dataframe(
            display_df.style.format({
                "final_price": "{:.0f}",
                "overall_rating": "{:.1f}",
                "value_score": "{:.4f}",
                "amenity_count": "{:.0f}",
            }),
            use_container_width=True,
        )


# ─────────────────────────── Page 3: Weather Dashboard ─────────────────────

def page_weather():
    st.title("🌡️ Weather Dashboard")
    st.markdown("Current conditions and daily forecasts for Egyptian cities.")

    # --- Current weather ---
    rows = run_query(f"""
        SELECT city, observed_at, temperature, is_day, rain, snowfall, showers,
               wind_speed, humidity, avg_temperature, avg_wind_speed
        FROM {SILVER}.stg_current_weather_data
        ORDER BY temperature DESC
    """)

    if not rows:
        st.warning("No weather data yet.")
        return

    cols = ["city", "observed_at", "temperature", "is_day", "rain", "snowfall",
            "showers", "wind_speed", "humidity", "avg_temperature", "avg_wind_speed"]
    df_cur = pd.DataFrame(rows, columns=cols)

    # --- Daily weather ---
    rows_daily = run_query(f"""
        SELECT city, daily_date, max_temp, min_temp, max_wind_speed, weather_code
        FROM {SILVER}.stg_daily_weather_data
        ORDER BY city, daily_date
    """)
    cols_d = ["city", "daily_date", "max_temp", "min_temp", "max_wind_speed", "weather_code"]
    df_daily = pd.DataFrame(rows_daily, columns=cols_d) if rows_daily else pd.DataFrame()

    # --- KPIs ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cities", len(df_cur))
    c2.metric("Hottest City", df_cur.iloc[0]["city"] if len(df_cur) else "—", f"{df_cur['temperature'].max():.1f}°C" if len(df_cur) else "—")
    c3.metric("Coldest City", df_cur.iloc[-1]["city"] if len(df_cur) else "—", f"{df_cur['temperature'].min():.1f}°C" if len(df_cur) else "—")
    c4.metric("Avg Humidity", f"{df_cur['humidity'].mean():.0f}%" if len(df_cur) else "—")

    st.markdown("---")

    # --- Current temperature bar ---
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df_cur, x="city", y="temperature", color="temperature",
            color_continuous_scale="RdYlBu_r",
            title="Current Temperature by City (°C)",
            text=df_cur["temperature"].round(1),
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(name="Humidity (%)", x=df_cur["city"], y=df_cur["humidity"], marker_color="#2ecc71"))
        fig2.add_trace(go.Bar(name="Wind Speed (km/h)", x=df_cur["city"], y=df_cur["wind_speed"], marker_color="#e74c3c"))
        fig2.update_layout(
            title="Humidity & Wind by City", barmode="group", height=350,
            xaxis_title="City", yaxis_title="Value",
        )
        st.plotly_chart(fig2, use_container_width=True)

    # --- Daily forecast ---
    if not df_daily.empty:
        st.markdown("### 📅 Daily Forecast")
        cities = sorted(df_daily["city"].unique())
        selected_city = st.selectbox("Select City", cities)
        city_data = df_daily[df_daily["city"] == selected_city]

        if not city_data.empty:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=city_data["daily_date"], y=city_data["max_temp"],
                mode="lines+markers", name="Max Temp (°C)",
                line=dict(color="#e74c3c"), hovertemplate="Date: %{x}<br>Max: %{y:.1f}°C<extra></extra>",
            ))
            fig3.add_trace(go.Scatter(
                x=city_data["daily_date"], y=city_data["min_temp"],
                mode="lines+markers", name="Min Temp (°C)",
                line=dict(color="#3498db"), hovertemplate="Date: %{x}<br>Min: %{y:.1f}°C<extra></extra>",
            ))
            fig3.update_layout(
                title=f"Temperature Forecast — {selected_city}",
                xaxis_title="Date", yaxis_title="Temperature (°C)", height=400,
            )
            st.plotly_chart(fig3, use_container_width=True)

            with st.expander("📋 Daily Data Table"):
                st.dataframe(city_data[["daily_date", "max_temp", "min_temp", "max_wind_speed", "weather_code"]], use_container_width=True)


# ─────────────────────────── Page 4: Dining & Landmarks ─────────────────────

def page_dining_landmarks():
    st.title("🍽️ Dining & Landmarks")
    st.markdown("Restaurant categories and tourist landmarks by city.")

    # --- Restaurants ---
    rows = run_query(f"""
        SELECT city, restaurant_name, category, area, latitude, longitude
        FROM {SILVER}.stg_restaurant_data
        ORDER BY city, category
    """)

    col1, col2 = st.columns([1, 1])

    if rows:
        cols = ["city", "restaurant_name", "category", "area", "latitude", "longitude"]
        df_r = pd.DataFrame(rows, columns=cols)

        with col1:
            st.subheader("🍽️ Restaurants")
            c1, c2 = st.columns(2)
            c1.metric("Total Restaurants", len(df_r))
            c2.metric("Cities", df_r["city"].nunique())

            # Category distribution
            cat_df = df_r["category"].value_counts().head(15).reset_index()
            cat_df.columns = ["category", "count"]
            fig = px.bar(
                cat_df, x="count", y="category", orientation="h",
                title="Top 15 Restaurant Categories", height=400,
                labels={"count": "Count", "category": "Category"},
            )
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)

            # City breakdown
            city_r = df_r.groupby("city").size().reset_index(name="count")
            city_r = city_r.sort_values("count", ascending=True)
            fig2 = px.bar(
                city_r, x="count", y="city", orientation="h",
                title="Restaurants by City", height=300,
                labels={"count": "Count", "city": "City"},
            )
            fig2.update_yaxes(autorange="reversed")
            st.plotly_chart(fig2, use_container_width=True)
    else:
        with col1:
            st.warning("No restaurant data yet.")

    # --- Landmarks ---
    rows_l = run_query(f"""
        SELECT city, landmark_name, category, score, address, latitude, longitude
        FROM {SILVER}.stg_famous_landmark_data
        ORDER BY score DESC
    """)

    with col2:
        st.subheader("📍 Landmarks")
        if rows_l:
            cols = ["city", "landmark_name", "category", "score", "address", "latitude", "longitude"]
            df_l = pd.DataFrame(rows_l, columns=cols)

            c1, c2 = st.columns(2)
            c1.metric("Total Landmarks", len(df_l))
            c2.metric("Cities", df_l["city"].nunique())

            # Landmarks by city
            city_l = df_l.groupby("city").size().reset_index(name="count")
            city_l = city_l.sort_values("count", ascending=True)
            fig = px.bar(
                city_l, x="count", y="city", orientation="h",
                title="Landmarks by City", height=300,
                labels={"count": "Count", "city": "City"},
            )
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)

            # Top landmarks by score
            top_l = df_l.nlargest(15, "score")[["landmark_name", "city", "score", "category"]]
            fig2 = px.bar(
                top_l, x="score", y="landmark_name", orientation="h", color="city",
                title="Top 15 Landmarks by Score", height=400,
                labels={"score": "Score", "landmark_name": "Landmark"},
            )
            fig2.update_yaxes(autorange="reversed")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("No landmark data yet.")

    # --- Combined data tables ---
    st.markdown("---")
    ct1, ct2 = st.columns(2)
    if rows:
        with ct1:
            with st.expander("📋 Restaurant Details"):
                st.dataframe(pd.DataFrame(rows, columns=["city", "restaurant_name", "category", "area", "latitude", "longitude"]), use_container_width=True)
    if rows_l:
        with ct2:
            with st.expander("📋 Landmark Details"):
                st.dataframe(pd.DataFrame(rows_l, columns=["city", "landmark_name", "category", "score", "address", "latitude", "longitude"]), use_container_width=True)


# ─────────────────────────── Routing ─────────────────────────────────────────

pages = {
    "📊 Tourism Index": page_tourism_index,
    "🏨 Hotel Analysis": page_hotel_analysis,
    "🌡️ Weather Dashboard": page_weather,
    "🍽️ Dining & Landmarks": page_dining_landmarks,
}

pages[page]()
