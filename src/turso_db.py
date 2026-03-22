"""Turso (libSQL) database layer using the HTTP pipeline API.

Mirrors the interface of db.py but talks to a remote Turso database
over HTTPS. No native driver needed — just httpx.
"""
from __future__ import annotations

import logging

import httpx
import pandas as pd

from src import PriceRecord, Station, settings

log = logging.getLogger(__name__)

_DDL_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS stations (
        station_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        brand TEXT,
        operator TEXT,
        address TEXT,
        postcode TEXT,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        distance_miles REAL NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_id TEXT NOT NULL REFERENCES stations(station_id),
        fuel_type TEXT NOT NULL,
        price_ppl REAL NOT NULL,
        price_updated_at TEXT NOT NULL,
        fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(station_id, fuel_type, price_updated_at)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_prices_station_fuel ON prices(station_id, fuel_type)",
    "CREATE INDEX IF NOT EXISTS idx_prices_fetched ON prices(fetched_at)",
]


class TursoDB:
    def __init__(self, url: str | None = None, token: str | None = None) -> None:
        libsql_url = url or settings.turso_url
        # Convert libsql:// to https://
        self._base_url = libsql_url.replace("libsql://", "https://")
        self._token = token or settings.turso_token
        self._client = httpx.Client(timeout=30.0)

    def _pipeline(self, statements: list[dict]) -> list[dict]:
        """Execute a batch of statements via the Turso pipeline API."""
        resp = self._client.post(
            f"{self._base_url}/v3/pipeline",
            headers={"Authorization": f"Bearer {self._token}"},
            json={"requests": [{"type": "execute", "stmt": s} for s in statements] + [{"type": "close"}]},
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        for i, r in enumerate(results):
            if r.get("type") == "error":
                log.error("Statement %d failed: %s", i, r.get("error"))
        return results

    def _execute(self, sql: str, args: list | None = None) -> dict:
        stmt: dict = {"sql": sql}
        if args:
            stmt["args"] = [_turso_arg(a) for a in args]
        results = self._pipeline([stmt])
        return results[0] if results else {}

    def _execute_many(self, sql: str, rows: list[list]) -> int:
        """Execute the same statement with multiple parameter sets."""
        if not rows:
            return 0
        statements = []
        for args in rows:
            stmt: dict = {"sql": sql}
            stmt["args"] = [_turso_arg(a) for a in args]
            statements.append(stmt)
        results = self._pipeline(statements)
        affected = 0
        for r in results:
            if r.get("type") == "ok":
                affected += r.get("response", {}).get("result", {}).get("affected_row_count", 0)
        return affected

    def _query(self, sql: str, args: list | None = None) -> list[dict]:
        """Execute a query and return rows as list of dicts."""
        result = self._execute(sql, args)
        if result.get("type") != "ok":
            return []
        res = result.get("response", {}).get("result", {})
        cols = [c["name"] for c in res.get("cols", [])]
        rows = []
        for row in res.get("rows", []):
            rows.append({col: cell.get("value") for col, cell in zip(cols, row)})
        return rows

    def _query_df(self, sql: str, args: list | None = None) -> pd.DataFrame:
        rows = self._query(sql, args)
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ---- Schema ----

    def init_schema(self) -> None:
        self._pipeline([{"sql": s} for s in _DDL_STATEMENTS])
        log.info("Turso schema initialized")

    # ---- Stations ----

    def upsert_stations_bulk(self, stations: list[Station]) -> None:
        sql = """INSERT OR REPLACE INTO stations
                 (station_id, name, brand, operator, address, postcode, latitude, longitude, distance_miles)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        rows = [
            [s.station_id, s.name, s.brand, s.operator, s.address, s.postcode, s.lat, s.lng, s.distance_miles]
            for s in stations
        ]
        self._execute_many(sql, rows)

    def get_tracked_stations(self) -> list[Station]:
        rows = self._query("SELECT * FROM stations ORDER BY distance_miles")
        return [
            Station(
                station_id=r["station_id"],
                name=r["name"],
                brand=r.get("brand"),
                operator=r.get("operator"),
                address=r.get("address"),
                postcode=r.get("postcode"),
                lat=float(r["latitude"]),
                lng=float(r["longitude"]),
                distance_miles=float(r["distance_miles"]),
            )
            for r in rows
        ]

    # ---- Prices ----

    def insert_prices_bulk(self, records: list[PriceRecord]) -> int:
        sql = """INSERT OR IGNORE INTO prices
                 (station_id, fuel_type, price_ppl, price_updated_at, fetched_at)
                 VALUES (?, ?, ?, ?, ?)"""
        rows = [
            [r.station_id, r.fuel_type, r.price_ppl, r.price_updated_at, r.fetched_at]
            for r in records
        ]
        return self._execute_many(sql, rows)

    def get_latest_prices(self) -> pd.DataFrame:
        return self._query_df(
            """SELECT p.station_id, s.name, s.brand, s.distance_miles,
                      p.fuel_type, p.price_ppl, p.price_updated_at
               FROM prices p
               JOIN stations s ON s.station_id = p.station_id
               WHERE (p.station_id, p.fuel_type, p.price_updated_at) IN (
                   SELECT station_id, fuel_type, MAX(price_updated_at)
                   FROM prices
                   GROUP BY station_id, fuel_type
               )
               ORDER BY p.fuel_type, p.price_ppl"""
        )

    def get_price_history(
        self,
        station_id: str | None = None,
        fuel_type: str | None = None,
        since: str | None = None,
    ) -> pd.DataFrame:
        query = """SELECT p.station_id, s.name, s.brand,
                          p.fuel_type, p.price_ppl, p.price_updated_at, p.fetched_at
                   FROM prices p
                   JOIN stations s ON s.station_id = p.station_id
                   WHERE 1=1"""
        args: list = []
        if station_id:
            query += " AND p.station_id = ?"
            args.append(station_id)
        if fuel_type:
            query += " AND p.fuel_type = ?"
            args.append(fuel_type)
        if since:
            query += " AND p.price_updated_at >= ?"
            args.append(since)
        query += " ORDER BY p.price_updated_at"
        return self._query_df(query, args or None)

    def close(self) -> None:
        self._client.close()


def _turso_arg(val) -> dict:
    """Convert a Python value to a Turso API argument dict."""
    if val is None:
        return {"type": "null"}
    if isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    if isinstance(val, float):
        return {"type": "float", "value": val}  # floats must be numeric, not string
    return {"type": "text", "value": str(val)}
