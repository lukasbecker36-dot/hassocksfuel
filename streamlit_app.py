from __future__ import annotations

import sqlite3
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from src.analysis import (
    brand_comparison,
    price_change_patterns,
    price_history,
    spread_analysis,
    station_ranking,
)
from src.db import get_latest_prices, init_db

st.set_page_config(page_title="Hassocks Fuel Prices", page_icon="⛽", layout="wide")
st.title("⛽ Hassocks Fuel Price Tracker")
st.caption("Diesel and unleaded prices within 5 miles of Hassocks, West Sussex")

DB_PATH = str(Path(__file__).resolve().parent / "data" / "fuel_prices.db")


@st.cache_resource
def get_conn() -> sqlite3.Connection:
    return init_db(DB_PATH)


conn = get_conn()

# ---- Sidebar filters ----
fuel_type = st.sidebar.selectbox("Fuel type", ["B7", "E10"], format_func=lambda x: {"B7": "Diesel (B7)", "E10": "Unleaded (E10)"}[x])
days = st.sidebar.slider("Days of history", 1, 90, 30)

if st.sidebar.button("🔄 Refresh data"):
    st.cache_resource.clear()
    st.rerun()

# ---- Current prices ----
st.header("Current Prices")
latest = get_latest_prices(conn)
if latest.empty:
    st.info("No price data yet. Run the poller first: `python -m src.poller`")
    st.stop()

for ft in ["B7", "E10"]:
    ft_data = latest[latest["fuel_type"] == ft].copy()
    if ft_data.empty:
        continue
    label = "Diesel (B7)" if ft == "B7" else "Unleaded (E10)"
    st.subheader(label)
    ft_data = ft_data.sort_values("price_ppl")
    cheapest = ft_data["price_ppl"].min()
    most_expensive = ft_data["price_ppl"].max()

    def highlight_price(val):
        if val == cheapest:
            return "background-color: #c6efce; color: #006100"
        if val == most_expensive:
            return "background-color: #ffc7ce; color: #9c0006"
        return ""

    display = ft_data[["name", "brand", "distance_miles", "price_ppl", "price_updated_at"]].rename(
        columns={
            "name": "Station",
            "brand": "Brand",
            "distance_miles": "Distance (mi)",
            "price_ppl": "Price (ppl)",
            "price_updated_at": "Last Updated",
        }
    )
    styled = display.style.applymap(highlight_price, subset=["Price (ppl)"])
    st.dataframe(styled, use_container_width=True, hide_index=True)

# ---- Price history ----
st.header("Price History")
hist = price_history(conn, fuel_type=fuel_type, days=days)
if not hist.empty:
    chart = (
        alt.Chart(hist)
        .mark_line(point=True)
        .encode(
            x=alt.X("price_updated_at:T", title="Date"),
            y=alt.Y("price_ppl:Q", title="Price (ppl)", scale=alt.Scale(zero=False)),
            color=alt.Color("name:N", title="Station"),
            tooltip=["name", "price_ppl", "price_updated_at:T"],
        )
        .interactive()
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No price history data for this period.")

# ---- Station ranking ----
st.header("Station Ranking")
ranking = station_ranking(conn, fuel_type=fuel_type, days=days)
if not ranking.empty:
    chart = (
        alt.Chart(ranking)
        .mark_bar()
        .encode(
            x=alt.X("avg_price_ppl:Q", title="Avg Price (ppl)", scale=alt.Scale(zero=False)),
            y=alt.Y("name:N", title="Station", sort="x"),
            color=alt.Color("brand:N", title="Brand"),
            tooltip=["name", "brand", "avg_price_ppl", "min_price_ppl", "max_price_ppl"],
        )
        .properties(height=max(200, len(ranking) * 30))
    )
    st.altair_chart(chart, use_container_width=True)

# ---- Spread analysis ----
st.header("Price Spread Over Time")
spread = spread_analysis(conn, fuel_type=fuel_type, days=days)
if not spread.empty:
    area_data = spread.melt(
        id_vars=["date", "n_stations"],
        value_vars=["min_price", "max_price"],
        var_name="metric",
        value_name="price_ppl",
    )
    chart = (
        alt.Chart(area_data)
        .mark_area(opacity=0.3)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("price_ppl:Q", title="Price (ppl)", scale=alt.Scale(zero=False)),
            color=alt.Color("metric:N", title="", scale=alt.Scale(domain=["min_price", "max_price"], range=["#2ca02c", "#d62728"])),
            tooltip=["date:T", "metric", "price_ppl"],
        )
        .interactive()
        .properties(height=300)
    )
    spread_line = (
        alt.Chart(spread)
        .mark_line(color="#ff7f0e", strokeDash=[5, 3])
        .encode(
            x="date:T",
            y=alt.Y("spread:Q", title="Spread (ppl)"),
            tooltip=["date:T", "spread"],
        )
    )
    col1, col2 = st.columns(2)
    with col1:
        st.altair_chart(chart, use_container_width=True)
    with col2:
        st.altair_chart(spread_line, use_container_width=True)

# ---- Brand comparison ----
st.header("Brand Comparison")
brands = brand_comparison(conn, fuel_type=fuel_type, days=days)
if not brands.empty:
    chart = (
        alt.Chart(brands)
        .mark_bar()
        .encode(
            x=alt.X("avg_price_ppl:Q", title="Avg Price (ppl)", scale=alt.Scale(zero=False)),
            y=alt.Y("brand:N", title="Brand", sort="x"),
            color=alt.value("#4c78a8"),
            tooltip=["brand", "avg_price_ppl", "n_stations", "premium_vs_cheapest"],
        )
        .properties(height=max(150, len(brands) * 35))
    )
    st.altair_chart(chart, use_container_width=True)

# ---- Price change patterns ----
st.header("Price Change Patterns")
changes = price_change_patterns(conn, fuel_type=fuel_type, days=days)
if not changes.empty:
    st.dataframe(
        changes.rename(columns={
            "name": "Station",
            "price_updated_at": "Changed At",
            "prev_price": "Old Price",
            "price_ppl": "New Price",
            "delta_ppl": "Change (ppl)",
            "hours_after_leader": "Hours After Leader",
        }),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No price changes detected in this period.")
