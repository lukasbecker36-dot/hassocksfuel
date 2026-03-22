from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import httpx
import pandas as pd

from src import PriceRecord, Settings, Station, settings as default_settings
from src.geo import filter_stations_by_radius

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Endpoint paths (relative to API base URL)
# ---------------------------------------------------------------------------
STATIONS_ENDPOINT = "/pfs"  # GET /pfs?batch-number=1
PRICES_ENDPOINT = "/pfs/fuel-prices"  # GET /pfs/fuel-prices?batch-number=1

# ---------------------------------------------------------------------------
# Response field mappings — adjust after inspecting actual API responses
# These are best guesses based on the developer portal CSV guide.
# On first run, the client logs the raw response keys so you can fix these.
# ---------------------------------------------------------------------------
FIELD_NODE_ID = "node_id"
FIELD_TRADING_NAME = "trading_name"
FIELD_BRAND_NAME = "brand_name"
FIELD_OPERATOR = "operator"
FIELD_LATITUDE = "location.latitude"
FIELD_LONGITUDE = "location.longitude"
FIELD_POSTCODE = "location.postcode"
FIELD_ADDRESS = "location.address_line_1"

FIELD_FUEL_TYPE = "fuel_type"
FIELD_PRICE_PPL = "price"
FIELD_PRICE_UPDATED = "price_last_updated"
FIELD_PRICE_STATION_ID = "node_id"
FIELD_FUEL_PRICES_KEY = "fuel_prices"  # nested array of prices per station

# Map API fuel types to our canonical names
FUEL_TYPE_MAP = {
    "E10": "E10",
    "B7_STANDARD": "B7",
    "B7": "B7",
    "E5": "E5",  # ignored unless needed
}
FUEL_TYPES_OF_INTEREST = {"E10", "B7"}

# CSV column mappings (for the twice-daily CSV download)
CSV_COL_NODE_ID = "node_id"
CSV_COL_NAME = "trading_name"
CSV_COL_BRAND = "brand_name"
CSV_COL_LAT = "location.latitude"
CSV_COL_LNG = "location.longitude"
CSV_COL_POSTCODE = "location.postcode"
CSV_COL_ADDRESS = "location.address_line_1"


class FuelFinderAPIError(Exception):
    pass


class AuthError(FuelFinderAPIError):
    pass


class RateLimitError(FuelFinderAPIError):
    pass


def _nested_get(d: dict, dotted_key: str):
    """Get a value from a nested dict using dot notation, e.g. 'location.latitude'."""
    keys = dotted_key.split(".")
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


class FuelFinderClient:
    def __init__(self, cfg: Settings | None = None) -> None:
        self._cfg = cfg or default_settings
        self._client = httpx.Client(
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
        )
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

    # ---- Auth ----

    def _authenticate(self) -> None:
        log.info("Authenticating with Fuel Finder API")
        resp = self._client.post(
            self._cfg.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._cfg.client_id,
                "client_secret": self._cfg.client_secret,
                "scope": "fuelfinder.read",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if resp.status_code != 200:
            raise AuthError(f"Auth failed: {resp.status_code} {resp.text}")
        body = resp.json()
        # Response may wrap token in a "data" key
        token_data = body.get("data", body) if isinstance(body, dict) else body
        self._access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        self._token_expires_at = time.monotonic() + expires_in - 60
        log.info("Authenticated, token expires in %ds", expires_in)

    def _ensure_auth(self) -> None:
        if time.monotonic() >= self._token_expires_at:
            self._authenticate()

    # ---- HTTP ----

    def _get(self, path: str, params: dict | None = None) -> dict | list:
        self._ensure_auth()
        url = self._cfg.api_base_url.rstrip("/") + path
        backoffs = [0.5, 1.0, 2.0]

        for attempt in range(len(backoffs) + 1):
            resp = self._client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {self._access_token}"},
            )
            if resp.status_code == 401 and attempt == 0:
                log.warning("Got 401, re-authenticating")
                self._authenticate()
                continue
            if resp.status_code == 404:
                return None  # type: ignore[return-value]
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < len(backoffs):
                    log.warning("Got %d, retrying in %.1fs", resp.status_code, backoffs[attempt])
                    time.sleep(backoffs[attempt])
                    continue
                raise RateLimitError(f"Request failed after retries: {resp.status_code}")
            resp.raise_for_status()
            return resp.json()

        raise FuelFinderAPIError("Request failed unexpectedly")

    def _get_all_batches(self, path: str) -> list[dict]:
        """Fetch all batches from a paginated endpoint using ?batch-number=N."""
        all_items: list[dict] = []
        batch = 1
        while True:
            data = self._get(path, params={"batch-number": batch})

            # 404 means no more batches
            if data is None:
                break

            # Extract items — handle both list and dict responses
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Try common wrapper keys, fall back to the whole dict
                for key in ("data", "results", "items", "forecourts", "prices", "pfs"):
                    if key in data and isinstance(data[key], list):
                        items = data[key]
                        break
                else:
                    # Log keys on first batch so we can fix the mapping
                    if batch == 1:
                        log.info("Response keys: %s", list(data.keys()))
                    items = [data]
            else:
                break

            if not items:
                break

            all_items.extend(items)
            log.info("Batch %d: %d items (total: %d)", batch, len(items), len(all_items))
            batch += 1

        return all_items

    # ---- Stations ----

    def _parse_station(self, raw: dict, distance_miles: float = 0.0) -> Station:
        return Station(
            station_id=str(_nested_get(raw, FIELD_NODE_ID) or ""),
            name=str(_nested_get(raw, FIELD_TRADING_NAME) or "Unknown"),
            brand=_nested_get(raw, FIELD_BRAND_NAME),
            operator=_nested_get(raw, FIELD_OPERATOR),
            address=_nested_get(raw, FIELD_ADDRESS),
            postcode=_nested_get(raw, FIELD_POSTCODE),
            lat=float(_nested_get(raw, FIELD_LATITUDE) or 0),
            lng=float(_nested_get(raw, FIELD_LONGITUDE) or 0),
            distance_miles=distance_miles,
        )

    def fetch_all_stations(self) -> list[dict]:
        """Fetch all PFS information (all batches)."""
        items = self._get_all_batches(STATIONS_ENDPOINT)
        if items:
            log.info("Sample station keys: %s", list(items[0].keys())[:15])
        return items

    def fetch_stations_near_hassocks(self) -> list[Station]:
        raw_stations = self.fetch_all_stations()
        if not raw_stations:
            return []

        # Flatten nested location fields for geo filtering
        for s in raw_stations:
            loc = s.get("location", {})
            if isinstance(loc, dict):
                s.setdefault("latitude", loc.get("latitude"))
                s.setdefault("longitude", loc.get("longitude"))

        nearby = filter_stations_by_radius(
            raw_stations,
            self._cfg.hassocks_lat,
            self._cfg.hassocks_lng,
            self._cfg.radius_miles,
            lat_key="latitude",
            lng_key="longitude",
        )
        return [self._parse_station(s, s["distance_miles"]) for s in nearby]

    # ---- Prices ----

    def fetch_all_prices_bulk(self, tracked_station_ids: set[str] | None = None) -> list[PriceRecord]:
        """Fetch all PFS fuel prices (all batches), optionally filtering to tracked stations.

        The API returns station records with nested fuel_prices arrays.
        """
        now = datetime.now(timezone.utc).isoformat()
        raw_stations = self._get_all_batches(PRICES_ENDPOINT)

        records: list[PriceRecord] = []
        for station in raw_stations:
            sid = str(station.get(FIELD_PRICE_STATION_ID, ""))
            if tracked_station_ids and sid not in tracked_station_ids:
                continue

            fuel_prices = station.get(FIELD_FUEL_PRICES_KEY, [])
            for p in fuel_prices:
                raw_fuel = str(p.get(FIELD_FUEL_TYPE, ""))
                canonical_fuel = FUEL_TYPE_MAP.get(raw_fuel)
                if canonical_fuel not in FUEL_TYPES_OF_INTEREST:
                    continue
                price_val = p.get(FIELD_PRICE_PPL)
                if price_val is None:
                    continue
                records.append(
                    PriceRecord(
                        station_id=sid,
                        fuel_type=canonical_fuel,
                        price_ppl=float(price_val),
                        price_updated_at=str(p.get(FIELD_PRICE_UPDATED, now)),
                        fetched_at=now,
                    )
                )

        log.info("Parsed %d price records for tracked stations", len(records))
        return records

    # ---- CSV fallback ----

    def load_stations_from_csv(self, csv_path: str) -> list[Station]:
        df = pd.read_csv(csv_path)
        stations_raw = df.to_dict("records")

        # Flatten column names for geo filtering
        lat_col = CSV_COL_LAT.split(".")[-1] if "." in CSV_COL_LAT else CSV_COL_LAT
        lng_col = CSV_COL_LNG.split(".")[-1] if "." in CSV_COL_LNG else CSV_COL_LNG

        for row in stations_raw:
            if lat_col not in row and CSV_COL_LAT in row:
                row[lat_col] = row[CSV_COL_LAT]
            if lng_col not in row and CSV_COL_LNG in row:
                row[lng_col] = row[CSV_COL_LNG]

        nearby = filter_stations_by_radius(
            stations_raw,
            self._cfg.hassocks_lat,
            self._cfg.hassocks_lng,
            self._cfg.radius_miles,
            lat_key=lat_col,
            lng_key=lng_col,
        )
        results = []
        for s in nearby:
            results.append(
                Station(
                    station_id=str(s.get(CSV_COL_NODE_ID, "")),
                    name=str(s.get(CSV_COL_NAME, "Unknown")),
                    brand=s.get(CSV_COL_BRAND),
                    operator=None,
                    address=s.get(CSV_COL_ADDRESS),
                    postcode=s.get(CSV_COL_POSTCODE),
                    lat=float(s.get(lat_col, 0)),
                    lng=float(s.get(lng_col, 0)),
                    distance_miles=s["distance_miles"],
                )
            )
        return results

    def close(self) -> None:
        self._client.close()
