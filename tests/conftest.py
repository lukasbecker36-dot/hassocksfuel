from __future__ import annotations

import pytest

from src import PriceRecord, Station
from src.db import init_db, insert_prices_bulk, upsert_stations_bulk


@pytest.fixture
def conn():
    c = init_db(":memory:")
    yield c
    c.close()


SAMPLE_STATIONS = [
    Station("S1", "Tesco Burgess Hill", 50.9530, -0.1290, 2.0, brand="Tesco", postcode="RH15 9AA"),
    Station("S2", "Shell Hassocks", 50.9250, -0.1500, 0.1, brand="Shell", postcode="BN6 8AA"),
    Station("S3", "BP Hurstpierpoint", 50.9350, -0.1800, 1.5, brand="BP", postcode="BN6 9UJ"),
]

SAMPLE_PRICES = [
    PriceRecord("S1", "B7", 142.9, "2026-03-20T10:00:00Z", "2026-03-20T10:05:00Z"),
    PriceRecord("S1", "E10", 137.9, "2026-03-20T10:00:00Z", "2026-03-20T10:05:00Z"),
    PriceRecord("S2", "B7", 145.9, "2026-03-20T10:00:00Z", "2026-03-20T10:05:00Z"),
    PriceRecord("S2", "E10", 139.9, "2026-03-20T10:00:00Z", "2026-03-20T10:05:00Z"),
    PriceRecord("S3", "B7", 143.5, "2026-03-20T10:00:00Z", "2026-03-20T10:05:00Z"),
    PriceRecord("S3", "E10", 138.5, "2026-03-20T10:00:00Z", "2026-03-20T10:05:00Z"),
    # Day 2 — S1 drops price
    PriceRecord("S1", "B7", 141.9, "2026-03-21T10:00:00Z", "2026-03-21T10:05:00Z"),
    PriceRecord("S2", "B7", 145.9, "2026-03-21T10:00:00Z", "2026-03-21T10:05:00Z"),
    PriceRecord("S3", "B7", 143.5, "2026-03-21T12:00:00Z", "2026-03-21T12:05:00Z"),
]


@pytest.fixture
def populated_conn(conn):
    upsert_stations_bulk(conn, SAMPLE_STATIONS)
    insert_prices_bulk(conn, SAMPLE_PRICES)
    return conn
