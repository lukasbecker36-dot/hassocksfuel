"""One-shot CSV-based poller — no OAuth needed, works from anywhere."""
from __future__ import annotations

import logging
import sys

import httpx

from src import settings
from src.csv_poller import download_csv, parse_csv_prices
from src.db import init_db, insert_prices_bulk, upsert_stations_bulk, get_tracked_stations
from src.turso_db import TursoDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    conn = init_db()

    turso: TursoDB | None = None
    if settings.turso_url and settings.turso_token:
        turso = TursoDB()
        turso.init_schema()

    client = httpx.Client(timeout=30.0)

    try:
        # Get tracked stations from local DB or Turso
        stations = get_tracked_stations(conn)
        if not stations and turso:
            log.info("Local DB empty, checking Turso for tracked stations...")
            stations = turso.get_tracked_stations()
            if stations:
                upsert_stations_bulk(conn, stations)
                log.info("Loaded %d stations from Turso", len(stations))

        tracked_ids = {s.station_id for s in stations}

        # Download and parse CSV
        csv_text = download_csv(client)
        new_stations, records = parse_csv_prices(csv_text, tracked_ids)

        # Save any newly discovered stations
        if new_stations:
            upsert_stations_bulk(conn, new_stations)
            if turso:
                turso.upsert_stations_bulk(new_stations)

        # Save prices
        new_count = insert_prices_bulk(conn, records)
        if turso and records:
            turso.insert_prices_bulk(records)

        log.info(
            "CSV poll complete: %d stations, %d price records parsed, %d new prices stored",
            len(tracked_ids) or len(new_stations),
            len(records),
            new_count,
        )
    except Exception:
        log.exception("CSV poll failed")
        sys.exit(1)
    finally:
        client.close()
        conn.close()
        if turso:
            turso.close()


if __name__ == "__main__":
    main()
