from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import schedule

from src import settings
from src.api_client import FuelFinderClient
from src.db import get_tracked_stations, init_db, insert_prices_bulk, upsert_stations_bulk
from src.turso_db import TursoDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


def poll_once(
    client: FuelFinderClient,
    conn: sqlite3.Connection,
    turso: TursoDB | None = None,
) -> None:
    stations = get_tracked_stations(conn)
    if not stations and turso:
        log.info("Local DB empty, checking Turso for tracked stations...")
        try:
            stations = turso.get_tracked_stations()
        except Exception:
            log.warning("Failed to fetch stations from Turso", exc_info=True)
            stations = []
        if stations:
            upsert_stations_bulk(conn, stations)
            log.info("Loaded %d stations from Turso", len(stations))

    if not stations:
        log.info("No stations tracked yet, discovering nearby stations...")
        stations = client.fetch_stations_near_hassocks()
        if not stations:
            log.warning("No stations found within %.1f miles", settings.radius_miles)
            return
        upsert_stations_bulk(conn, stations)
        if turso:
            try:
                turso.upsert_stations_bulk(stations)
            except Exception:
                log.warning("Failed to sync stations to Turso", exc_info=True)
        log.info("Discovered %d stations", len(stations))

    station_ids = {s.station_id for s in stations}
    records = client.fetch_all_prices_bulk(tracked_station_ids=station_ids)
    new_count = insert_prices_bulk(conn, records)

    if turso and records:
        try:
            turso.insert_prices_bulk(records)
        except Exception:
            log.warning("Failed to sync prices to Turso", exc_info=True)

    # Flag stations with stale prices (not updated in over 2 days)
    stale_cutoff = datetime.now(timezone.utc) - timedelta(days=2)
    station_names = {s.station_id: s.name for s in stations}
    for r in records:
        try:
            updated = datetime.fromisoformat(r.price_updated_at.replace("Z", "+00:00"))
            if updated < stale_cutoff:
                log.warning(
                    "Stale price: %s (%s) %s %.1fp last updated %s",
                    station_names.get(r.station_id, r.station_id),
                    r.station_id,
                    r.fuel_type,
                    r.price_ppl,
                    r.price_updated_at,
                )
        except (ValueError, TypeError):
            pass

    log.info(
        "Poll complete: %d stations, %d price records fetched, %d new prices stored",
        len(stations),
        len(records),
        new_count,
    )


def refresh_stations(
    client: FuelFinderClient,
    conn: sqlite3.Connection,
    turso: TursoDB | None = None,
) -> None:
    try:
        stations = client.fetch_stations_near_hassocks()
        if stations:
            upsert_stations_bulk(conn, stations)
            if turso:
                turso.upsert_stations_bulk(stations)
            log.info("Refreshed station list: %d stations", len(stations))
    except Exception:
        log.exception("Station refresh failed")


def main() -> None:
    log.info("Starting Hassocks Fuel Price Tracker")
    log.info(
        "Centre: (%.4f, %.4f), radius: %.1f miles, poll interval: %d min",
        settings.hassocks_lat,
        settings.hassocks_lng,
        settings.radius_miles,
        settings.poll_interval_minutes,
    )

    conn = init_db()
    client = FuelFinderClient()

    turso: TursoDB | None = None
    if settings.turso_url and settings.turso_token:
        turso = TursoDB()
        turso.init_schema()
        log.info("Turso remote DB connected")

    def _safe_poll() -> None:
        try:
            poll_once(client, conn, turso)
        except Exception:
            log.exception("Scheduled poll failed")

    def _safe_refresh() -> None:
        try:
            refresh_stations(client, conn, turso)
        except Exception:
            log.exception("Scheduled station refresh failed")

    try:
        poll_once(client, conn, turso)

        schedule.every(settings.poll_interval_minutes).minutes.do(_safe_poll)
        schedule.every(7).days.do(_safe_refresh)

        log.info("Scheduler running — next poll in %d minutes", settings.poll_interval_minutes)
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        client.close()
        conn.close()
        if turso:
            turso.close()


if __name__ == "__main__":
    main()
