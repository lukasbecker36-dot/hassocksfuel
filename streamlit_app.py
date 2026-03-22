from __future__ import annotations

import os
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Hassocks Fuel Prices", page_icon="\u26fd", layout="wide")
st.title("\u26fd Hassocks Fuel Price Tracker")
st.caption("Diesel and unleaded prices within 5 miles of Hassocks, West Sussex")


# ---- Data source: Turso (cloud) or local SQLite ----

def _use_turso() -> bool:
    """Use Turso if credentials are available (Streamlit Cloud or local .env)."""
    # Streamlit Cloud secrets
    if hasattr(st, "secrets"):
        try:
            _ = st.secrets["TURSO_DATABASE_URL"]
            return True
        except (KeyError, FileNotFoundError):
            pass
    # Local .env
    return bool(os.environ.get("TURSO_DATABASE_URL"))


if _use_turso():
    from src.turso_db import TursoDB

    @st.cache_resource
    def get_turso() -> TursoDB:
        # Prefer st.secrets, fall back to env vars
        try:
            url = st.secrets["TURSO_DATABASE_URL"]
            token = st.secrets["TURSO_AUTH_TOKEN"]
        except (KeyError, FileNotFoundError):
            url = os.environ["TURSO_DATABASE_URL"]
            token = os.environ["TURSO_AUTH_TOKEN"]
        return TursoDB(url=url, token=token)

    turso = get_turso()

    def get_latest_prices() -> pd.DataFrame:
        return turso.get_latest_prices()

    def get_price_history(fuel_type: str, days: int) -> pd.DataFrame:
        from datetime import datetime, timedelta, timezone
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        return turso.get_price_history(fuel_type=fuel_type, since=since)

else:
    import sqlite3
    from src.db import get_latest_prices as _get_latest, get_price_history as _get_history, init_db

    DB_PATH = str(Path(__file__).resolve().parent / "data" / "fuel_prices.db")

    @st.cache_resource
    def get_conn() -> sqlite3.Connection:
        return init_db(DB_PATH)

    _conn = get_conn()

    def get_latest_prices() -> pd.DataFrame:
        return _get_latest(_conn)

    def get_price_history(fuel_type: str, days: int) -> pd.DataFrame:
        from datetime import datetime, timedelta, timezone
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        return _get_history(_conn, fuel_type=fuel_type, since=since)


# ---- Sidebar filters ----
fuel_type = st.sidebar.selectbox(
    "Fuel type", ["B7", "E10"],
    format_func=lambda x: {"B7": "Diesel (B7)", "E10": "Unleaded (E10)"}[x],
)
days = st.sidebar.slider("Days of history", 1, 90, 30)

if st.sidebar.button("\U0001f504 Refresh data"):
    st.cache_resource.clear()
    st.rerun()

# ---- Current prices ----
st.header("Current Prices")
latest = get_latest_prices()

# Ensure numeric types
if not latest.empty:
    latest["price_ppl"] = pd.to_numeric(latest["price_ppl"], errors="coerce")
    latest["distance_miles"] = pd.to_numeric(latest["distance_miles"], errors="coerce")

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

    def highlight_price(val, _lo=cheapest, _hi=most_expensive):
        if val == _lo:
            return "background-color: #c6efce; color: #006100"
        if val == _hi:
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
hist = get_price_history(fuel_type=fuel_type, days=days)
if not hist.empty:
    hist["price_ppl"] = pd.to_numeric(hist["price_ppl"], errors="coerce")
    hist["price_updated_at"] = pd.to_datetime(hist["price_updated_at"])
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
if not hist.empty:
    ranking = (
        hist.groupby(["name", "brand"])["price_ppl"]
        .agg(["mean", "min", "max"])
        .reset_index()
        .rename(columns={"mean": "avg_price_ppl", "min": "min_price_ppl", "max": "max_price_ppl"})
        .sort_values("avg_price_ppl")
    )
    ranking["rank"] = range(1, len(ranking) + 1)
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
if not hist.empty:
    spread = (
        hist.groupby(hist["price_updated_at"].dt.date)["price_ppl"]
        .agg(["min", "max", "count"])
        .reset_index()
        .rename(columns={"price_updated_at": "date", "min": "min_price", "max": "max_price", "count": "n_stations"})
    )
    spread["spread"] = spread["max_price"] - spread["min_price"]
    spread["date"] = pd.to_datetime(spread["date"])

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
if not hist.empty:
    brands = (
        hist.groupby(hist["brand"].fillna("Independent"))["price_ppl"]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"brand": "brand", "mean": "avg_price_ppl", "count": "n_observations"})
        .sort_values("avg_price_ppl")
    )
    cheapest_brand = brands["avg_price_ppl"].min()
    brands["premium_vs_cheapest"] = (brands["avg_price_ppl"] - cheapest_brand).round(1)

    chart = (
        alt.Chart(brands)
        .mark_bar()
        .encode(
            x=alt.X("avg_price_ppl:Q", title="Avg Price (ppl)", scale=alt.Scale(zero=False)),
            y=alt.Y("brand:N", title="Brand", sort="x"),
            color=alt.value("#4c78a8"),
            tooltip=["brand", "avg_price_ppl", "n_observations", "premium_vs_cheapest"],
        )
        .properties(height=max(150, len(brands) * 35))
    )
    st.altair_chart(chart, use_container_width=True)

# ---- Price change patterns ----
st.header("Price Change Patterns")
if not hist.empty:
    hist_sorted = hist.sort_values(["station_id", "price_updated_at"])
    hist_sorted["prev_price"] = hist_sorted.groupby("station_id")["price_ppl"].shift(1)
    changes = hist_sorted[hist_sorted["price_ppl"] != hist_sorted["prev_price"]].dropna(subset=["prev_price"]).copy()

    if not changes.empty:
        changes["delta_ppl"] = changes["price_ppl"] - changes["prev_price"]
        changes["date"] = changes["price_updated_at"].dt.date
        daily_first = changes.groupby("date")["price_updated_at"].min().reset_index()
        daily_first.columns = ["date", "leader_time"]
        changes = changes.merge(daily_first, on="date", how="left")
        changes["hours_after_leader"] = (
            (changes["price_updated_at"] - changes["leader_time"]).dt.total_seconds() / 3600
        ).round(1)

        st.dataframe(
            changes[["name", "price_updated_at", "prev_price", "price_ppl", "delta_ppl", "hours_after_leader"]].rename(
                columns={
                    "name": "Station",
                    "price_updated_at": "Changed At",
                    "prev_price": "Old Price",
                    "price_ppl": "New Price",
                    "delta_ppl": "Change (ppl)",
                    "hours_after_leader": "Hours After Leader",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No price changes detected in this period.")
