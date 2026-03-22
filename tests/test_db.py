from __future__ import annotations

from src import PriceRecord, Station
from src.db import get_latest_prices, get_tracked_stations, insert_prices_bulk, upsert_station, upsert_stations_bulk

from .conftest import SAMPLE_PRICES, SAMPLE_STATIONS


def test_init_creates_tables(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = [t["name"] for t in tables]
    assert "stations" in names
    assert "prices" in names


def test_upsert_and_get_stations(conn):
    upsert_station(conn, SAMPLE_STATIONS[0])
    result = get_tracked_stations(conn)
    assert len(result) == 1
    assert result[0].station_id == "S1"
    assert result[0].name == "Tesco Burgess Hill"


def test_upsert_stations_bulk(conn):
    upsert_stations_bulk(conn, SAMPLE_STATIONS)
    result = get_tracked_stations(conn)
    assert len(result) == 3
    # Should be sorted by distance
    assert result[0].distance_miles <= result[1].distance_miles


def test_insert_prices_dedup(conn):
    upsert_stations_bulk(conn, SAMPLE_STATIONS)
    count1 = insert_prices_bulk(conn, SAMPLE_PRICES)
    assert count1 == len(SAMPLE_PRICES)
    # Insert again — all should be ignored
    count2 = insert_prices_bulk(conn, SAMPLE_PRICES)
    assert count2 == 0


def test_get_latest_prices(populated_conn):
    df = get_latest_prices(populated_conn)
    assert not df.empty
    # S1 diesel should be 141.9 (the day-2 price)
    s1_diesel = df[(df["station_id"] == "S1") & (df["fuel_type"] == "B7")]
    assert len(s1_diesel) == 1
    assert s1_diesel.iloc[0]["price_ppl"] == 141.9
