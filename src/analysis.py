from __future__ import annotations

import sqlite3

import pandas as pd


def price_history(
    conn: sqlite3.Connection,
    station_id: str | None = None,
    fuel_type: str = "B7",
    days: int = 30,
) -> pd.DataFrame:
    query = """
        SELECT s.name, s.brand, p.fuel_type, p.price_ppl, p.price_updated_at
        FROM prices p
        JOIN stations s ON s.station_id = p.station_id
        WHERE p.fuel_type = ?
          AND p.price_updated_at >= datetime('now', ?)
    """
    params: list = [fuel_type, f"-{days} days"]
    if station_id:
        query += " AND p.station_id = ?"
        params.append(station_id)
    query += " ORDER BY p.price_updated_at"
    df = pd.read_sql_query(query, conn, params=params)
    if not df.empty:
        df["price_updated_at"] = pd.to_datetime(df["price_updated_at"])
    return df


def station_ranking(
    conn: sqlite3.Connection,
    fuel_type: str = "B7",
    days: int = 30,
) -> pd.DataFrame:
    query = """
        SELECT s.name, s.brand, s.distance_miles,
               AVG(p.price_ppl) AS avg_price_ppl,
               MIN(p.price_ppl) AS min_price_ppl,
               MAX(p.price_ppl) AS max_price_ppl,
               COUNT(*) AS n_observations
        FROM prices p
        JOIN stations s ON s.station_id = p.station_id
        WHERE p.fuel_type = ?
          AND p.price_updated_at >= datetime('now', ?)
        GROUP BY s.station_id
        ORDER BY avg_price_ppl
    """
    df = pd.read_sql_query(query, conn, params=[fuel_type, f"-{days} days"])
    if not df.empty:
        df["rank"] = range(1, len(df) + 1)
    return df


def spread_analysis(
    conn: sqlite3.Connection,
    fuel_type: str = "B7",
    days: int = 30,
) -> pd.DataFrame:
    query = """
        SELECT DATE(p.price_updated_at) AS date,
               MIN(p.price_ppl) AS min_price,
               MAX(p.price_ppl) AS max_price,
               MAX(p.price_ppl) - MIN(p.price_ppl) AS spread,
               COUNT(DISTINCT p.station_id) AS n_stations
        FROM prices p
        WHERE p.fuel_type = ?
          AND p.price_updated_at >= datetime('now', ?)
        GROUP BY DATE(p.price_updated_at)
        ORDER BY date
    """
    df = pd.read_sql_query(query, conn, params=[fuel_type, f"-{days} days"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def brand_comparison(
    conn: sqlite3.Connection,
    fuel_type: str = "B7",
    days: int = 30,
) -> pd.DataFrame:
    query = """
        SELECT COALESCE(s.brand, 'Independent') AS brand,
               AVG(p.price_ppl) AS avg_price_ppl,
               COUNT(DISTINCT s.station_id) AS n_stations,
               COUNT(*) AS n_observations
        FROM prices p
        JOIN stations s ON s.station_id = p.station_id
        WHERE p.fuel_type = ?
          AND p.price_updated_at >= datetime('now', ?)
        GROUP BY COALESCE(s.brand, 'Independent')
        ORDER BY avg_price_ppl
    """
    df = pd.read_sql_query(query, conn, params=[fuel_type, f"-{days} days"])
    if not df.empty:
        cheapest = df["avg_price_ppl"].min()
        df["premium_vs_cheapest"] = (df["avg_price_ppl"] - cheapest).round(1)
    return df


def price_change_patterns(
    conn: sqlite3.Connection,
    fuel_type: str = "B7",
    days: int = 30,
) -> pd.DataFrame:
    """Detect price changes per station and compute lag vs first mover."""
    query = """
        SELECT p.station_id, s.name, p.price_ppl, p.price_updated_at
        FROM prices p
        JOIN stations s ON s.station_id = p.station_id
        WHERE p.fuel_type = ?
          AND p.price_updated_at >= datetime('now', ?)
        ORDER BY p.station_id, p.price_updated_at
    """
    df = pd.read_sql_query(query, conn, params=[fuel_type, f"-{days} days"])
    if df.empty:
        return df

    df["price_updated_at"] = pd.to_datetime(df["price_updated_at"])
    df["prev_price"] = df.groupby("station_id")["price_ppl"].shift(1)
    changes = df[df["price_ppl"] != df["prev_price"]].dropna(subset=["prev_price"]).copy()

    if changes.empty:
        return changes

    changes["delta_ppl"] = changes["price_ppl"] - changes["prev_price"]
    changes["date"] = changes["price_updated_at"].dt.date

    # For each day, find the earliest change (leader)
    daily_first = changes.groupby("date")["price_updated_at"].min().reset_index()
    daily_first.columns = ["date", "leader_time"]
    changes = changes.merge(daily_first, on="date", how="left")
    changes["hours_after_leader"] = (
        (changes["price_updated_at"] - changes["leader_time"]).dt.total_seconds() / 3600
    ).round(1)

    return changes[["name", "price_updated_at", "prev_price", "price_ppl", "delta_ppl", "hours_after_leader"]]
