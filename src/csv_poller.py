"""CSV-based poller that downloads prices from the Fuel Finder CSV endpoint.

The CSV is publicly available (no OAuth) so it works from GitHub Actions
and other cloud environments where the API's OAuth endpoint blocks
datacenter IPs.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from datetime import datetime, timezone

import httpx

from src import PriceRecord, Station, settings as default_settings, Settings
from src.geo import filter_stations_by_radius

log = logging.getLogger(__name__)

PRESIGNED_URL_ENDPOINT = (
    "https://www.fuel-finder.service.gov.uk/internal/v1.0.2/csv/generate-presigned-url"
)

# AES key used by the Fuel Finder frontend JS to decrypt responses.
_RAW_KEY = "8762dae892591b98df04f6badb39550ded3aec52e1227f816367af8d3064ba22"

# CSV column prefixes
_P = "forecourts."  # all columns are prefixed with this

# CSV fuel price columns → our canonical fuel types
# CSV has: forecourts.fuel_price.E10, forecourts.fuel_price.B7S (B7 Standard)
CSV_FUEL_COLS = {
    "E10": "E10",
    "B7S": "B7",
}


def _decrypt_response(hex_payload: str, iv_hex: str) -> dict:
    """Decrypt an AES-256-CBC encrypted response from the Fuel Finder API."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives import padding

    key = hashlib.sha256(_RAW_KEY.encode()).digest()
    iv = bytes.fromhex(iv_hex)
    ciphertext = bytes.fromhex(hex_payload)

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    padded = decryptor.update(ciphertext) + decryptor.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    plaintext = unpadder.update(padded) + unpadder.finalize()
    return json.loads(plaintext)


_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.fuel-finder.service.gov.uk/access-latest-fuelprices",
    "Origin": "https://www.fuel-finder.service.gov.uk",
}


def _get_csv_download_url(client: httpx.Client) -> str:
    """Get the presigned S3 URL for the CSV download."""
    resp = client.get(PRESIGNED_URL_ENDPOINT, headers=_BROWSER_HEADERS)
    resp.raise_for_status()
    body = resp.json()

    # Response is encrypted — decrypt it
    if "nxhex" in body and "iv" in body:
        decrypted = _decrypt_response(body["nxhex"], body["iv"])
        url = decrypted.get("data", {}).get("redirectUrl")
        if not url:
            raise ValueError(f"No redirectUrl in decrypted response: {list(decrypted.keys())}")
        return url

    # Maybe it's not encrypted (unlikely but handle it)
    url = body.get("data", {}).get("redirectUrl") or body.get("redirectUrl")
    if not url:
        raise ValueError(f"Cannot find CSV URL in response: {list(body.keys())}")
    return url


def download_csv(client: httpx.Client) -> str:
    """Download the CSV content as a string."""
    url = _get_csv_download_url(client)
    log.info("Downloading CSV from presigned URL")
    resp = client.get(url)
    resp.raise_for_status()
    return resp.text


def parse_csv_prices(
    csv_text: str,
    tracked_station_ids: set[str],
    cfg: Settings | None = None,
) -> tuple[list[Station], list[PriceRecord]]:
    """Parse the CSV text and return stations + price records for tracked stations.

    If tracked_station_ids is empty, discovers stations near Hassocks and returns those.
    """
    cfg = cfg or default_settings
    now = datetime.now(timezone.utc).isoformat()

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)

    if not rows:
        log.warning("CSV is empty")
        return [], []

    log.info("CSV contains %d rows", len(rows))

    # Discover stations if none tracked yet
    stations: list[Station] = []
    if not tracked_station_ids:
        nearby = filter_stations_by_radius(
            rows,
            cfg.hassocks_lat,
            cfg.hassocks_lng,
            cfg.radius_miles,
            lat_key=f"{_P}location.latitude",
            lng_key=f"{_P}location.longitude",
        )
        for s in nearby:
            station = _parse_station_row(s)
            if station:
                stations.append(station)
                tracked_station_ids.add(station.station_id)
        log.info("Discovered %d stations near Hassocks from CSV", len(stations))

    # Parse prices for tracked stations
    records: list[PriceRecord] = []
    for row in rows:
        sid = row.get(f"{_P}node_id", "").strip()
        if sid not in tracked_station_ids:
            continue
        _extract_prices_from_row(row, sid, now, records)

    log.info("Parsed %d price records from CSV for %d tracked stations",
             len(records), len(tracked_station_ids))
    return stations, records


def _parse_station_row(row: dict) -> Station | None:
    """Parse a CSV row into a Station."""
    sid = row.get(f"{_P}node_id", "").strip()
    if not sid:
        return None
    try:
        lat = float(row.get(f"{_P}location.latitude", 0))
        lng = float(row.get(f"{_P}location.longitude", 0))
    except (ValueError, TypeError):
        return None
    return Station(
        station_id=sid,
        name=row.get(f"{_P}trading_name", "Unknown").strip(),
        brand=row.get(f"{_P}brand_name", "").strip() or None,
        operator=None,
        address=row.get(f"{_P}location.address_line_1", "").strip() or None,
        postcode=row.get(f"{_P}location.postcode", "").strip() or None,
        lat=lat,
        lng=lng,
        distance_miles=row.get("distance_miles", 0),
    )


def _extract_prices_from_row(
    row: dict, station_id: str, fetched_at: str, records: list[PriceRecord]
) -> None:
    """Extract fuel price records from a CSV row.

    CSV columns: forecourts.fuel_price.E10, forecourts.fuel_price.B7S, etc.
    Timestamps: forecourts.price_change_effective_timestamp.E10, etc.
    """
    for csv_fuel, canonical in CSV_FUEL_COLS.items():
        price_val = row.get(f"{_P}fuel_price.{csv_fuel}", "").strip()
        if not price_val:
            continue
        try:
            ppl = float(price_val)
        except (ValueError, TypeError):
            continue
        updated_at = (
            row.get(f"{_P}price_change_effective_timestamp.{csv_fuel}", "").strip()
            or fetched_at
        )
        records.append(PriceRecord(
            station_id=station_id,
            fuel_type=canonical,
            price_ppl=ppl,
            price_updated_at=updated_at,
            fetched_at=fetched_at,
        ))
