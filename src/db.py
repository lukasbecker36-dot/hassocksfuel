from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from src import PriceRecord, Station, settings

_DDL = """\
CREATE TABLE IF NOT EXISTS stations (
    station_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    brand TEXT,
    operator TEXT,
    address TEXT,
    postcode TEXT,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    distance_miles REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL REFERENCES stations(station_id),
    fuel_type TEXT NOT NULL,
    price_ppl REAL NOT NULL,
    price_updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(station_id, fuel_type, price_updated_at)
);

CREATE INDEX IF NOT EXISTS idx_prices_station_fuel ON prices(station_id, fuel_type);
CREATE INDEX IF NOT EXISTS idx_prices_fetched ON prices(fetched_at);
"""


def init_db(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or settings.db_path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_DDL)
    conn.row_factory = sqlite3.Row
    return conn


def upsert_station(conn: sqlite3.Connection, station: Station) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO stations
           (station_id, name, brand, operator, address, postcode, latitude, longitude, distance_miles)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            station.station_id,
            station.name,
            station.brand,
            station.operator,
            station.address,
            station.postcode,
            station.lat,
            station.lng,
            station.distance_miles,
        ),
    )
    conn.commit()


def upsert_stations_bulk(conn: sqlite3.Connection, stations: list[Station]) -> None:
    conn.executemany(
        """INSERT OR REPLACE INTO stations
           (station_id, name, brand, operator, address, postcode, latitude, longitude, distance_miles)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (s.station_id, s.name, s.brand, s.operator, s.address, s.postcode, s.lat, s.lng, s.distance_miles)
            for s in stations
        ],
    )
    conn.commit()


def insert_prices_bulk(conn: sqlite3.Connection, records: list[PriceRecord]) -> int:
    cur = conn.executemany(
        """INSERT INTO prices
           (station_id, fuel_type, price_ppl, price_updated_at, fetched_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(station_id, fuel_type, price_updated_at)
           DO UPDATE SET price_ppl = excluded.price_ppl,
                         fetched_at = excluded.fetched_at""",
        [
            (r.station_id, r.fuel_type, r.price_ppl, r.price_updated_at, r.fetched_at)
            for r in records
        ],
    )
    conn.commit()
    return cur.rowcount


def get_tracked_stations(conn: sqlite3.Connection) -> list[Station]:
    rows = conn.execute("SELECT * FROM stations ORDER BY distance_miles").fetchall()
    return [
        Station(
            station_id=r["station_id"],
            name=r["name"],
            brand=r["brand"],
            operator=r["operator"],
            address=r["address"],
            postcode=r["postcode"],
            lat=r["latitude"],
            lng=r["longitude"],
            distance_miles=r["distance_miles"],
        )
        for r in rows
    ]


def get_latest_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """SELECT p.station_id, s.name, s.brand, s.distance_miles,
                  p.fuel_type, p.price_ppl, p.price_updated_at
           FROM prices p
           JOIN stations s ON s.station_id = p.station_id
           WHERE (p.station_id, p.fuel_type, p.price_updated_at) IN (
               SELECT station_id, fuel_type, MAX(price_updated_at)
               FROM prices
               GROUP BY station_id, fuel_type
           )
           ORDER BY p.fuel_type, p.price_ppl""",
        conn,
    )


def get_price_history(
    conn: sqlite3.Connection,
    station_id: str | None = None,
    fuel_type: str | None = None,
    since: str | None = None,
) -> pd.DataFrame:
    query = """SELECT p.station_id, s.name, s.brand,
                      p.fuel_type, p.price_ppl, p.price_updated_at, p.fetched_at
               FROM prices p
               JOIN stations s ON s.station_id = p.station_id
               WHERE 1=1"""
    params: list = []
    if station_id:
        query += " AND p.station_id = ?"
        params.append(station_id)
    if fuel_type:
        query += " AND p.fuel_type = ?"
        params.append(fuel_type)
    if since:
        query += " AND p.price_updated_at >= ?"
        params.append(since)
    query += " ORDER BY p.price_updated_at"
    return pd.read_sql_query(query, conn, params=params)
