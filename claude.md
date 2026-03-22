# Hassocks Fuel Price Tracker

## Project overview

This project tracks diesel and unleaded (E10) prices at service stations within 5 miles of Hassocks, West Sussex (lat: 50.9246, lng: -0.1507) over time, building a historical dataset of relative pricing between local stations.

The primary data source is the **UK Government Fuel Finder API**, the statutory open data scheme launched 2 February 2026 under The Motor Fuel Price (Open Data) Regulations 2025. All UK forecourts are legally required to report price changes within 30 minutes.

## Data source

### Fuel Finder API

- **Developer portal**: https://www.developer.fuel-finder.service.gov.uk/access-latest-fuelprices
- **Auth**: OAuth 2.0 client credentials flow. Register via GOV.UK One Login to obtain a client ID and secret.
- **Protocol**: REST API with JSON responses. Read-only GET requests to stable resource URLs.
- **Data available**: current retail prices by fuel type, forecourt details (address, operator, brand), site amenities, opening hours, and per-price update timestamps.
- **Update frequency**: prices published within 30 minutes of any change at the forecourt. The API reflects near-real-time data.
- **CSV fallback**: a CSV file with current prices and forecourt details is also available, updated twice daily. This can be used as a simpler bootstrapping option or fallback if the API auth flow is problematic.
- **Fuel types of interest**: `E10` (unleaded) and `B7` (diesel). The scheme also covers E5, super diesel, B10, and HVO but these are out of scope unless a local station only sells E5 as its unleaded grade.

### Fallback: `uk-fuel-prices-api` (PyPI)

If the official API proves awkward for rapid prototyping, the `uk-fuel-prices-api` Python package aggregates data from the CMA interim scheme feeds. It supports radius-based station lookups and sorting by fuel type. Note: coverage is less complete than the statutory Fuel Finder scheme and most feeds update only every 24 hours.

```python
from uk_fuel_prices_api import UKFuelPricesApi
api = UKFuelPricesApi()
await api.get_prices()
stations = api.stationsWithinRadius(50.9246, -0.1507, 5)
sorted_stations = api.sortByPrice(stations, "E10")
```

## Architecture

### Language and stack

- **Python 3.12+**
- `httpx` or `requests` for API calls
- `sqlite3` (stdlib) for local time-series storage — keep it simple, no database server
- `pandas` for analysis and export
- `schedule` or `cron` for periodic polling

### Directory structure

```
hassocks-fuel/
├── claude.md              # This file
├── src/
│   ├── __init__.py
│   ├── api_client.py      # Fuel Finder API auth + data fetching
│   ├── db.py              # SQLite schema, insert, query helpers
│   ├── geo.py             # Haversine filtering for 5-mile radius from Hassocks
│   ├── poller.py          # Scheduled polling loop
│   └── analysis.py        # Price comparison, time series, station ranking
├── data/
│   └── fuel_prices.db     # SQLite database (gitignored)
├── notebooks/
│   └── exploration.ipynb  # Ad-hoc analysis
├── tests/
│   └── ...
├── .env                   # API credentials (gitignored)
├── .gitignore
└── requirements.txt
```

### Database schema

```sql
CREATE TABLE stations (
    station_id TEXT PRIMARY KEY,     -- Fuel Finder forecourt ID
    name TEXT NOT NULL,
    brand TEXT,
    operator TEXT,
    address TEXT,
    postcode TEXT,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    distance_miles REAL NOT NULL     -- from Hassocks centre
);

CREATE TABLE prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL REFERENCES stations(station_id),
    fuel_type TEXT NOT NULL,          -- 'E10' or 'B7'
    price_ppl REAL NOT NULL,          -- pence per litre
    price_updated_at TEXT NOT NULL,   -- ISO 8601, from the API's update timestamp
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(station_id, fuel_type, price_updated_at)
);

CREATE INDEX idx_prices_station_fuel ON prices(station_id, fuel_type);
CREATE INDEX idx_prices_fetched ON prices(fetched_at);
```

The `UNIQUE` constraint on `(station_id, fuel_type, price_updated_at)` deduplicates — if the station hasn't changed its price since the last poll, the insert is skipped via `INSERT OR IGNORE`.

### Geo filtering

Hassocks centre: **50.9246°N, 0.1507°W**.

Use the haversine formula to filter stations within a 5-mile (8.05 km) radius. This filtering should happen client-side after fetching a broader region from the API (or from the CSV). Cache the station list and only re-check periodically (e.g. weekly) since stations don't appear/disappear often.

### Polling strategy

- Poll every **30 minutes** to match the mandatory reporting cadence.
- On each poll: fetch current prices for all tracked stations, insert new rows where `price_updated_at` has changed.
- Log poll results (stations checked, new prices recorded, errors) to stdout.
- Handle API errors gracefully — log and retry on next cycle, don't crash the loop.

## Key analysis outputs

1. **Price history per station**: time series of diesel and unleaded prices for each station, with daily/weekly aggregation.
2. **Relative ranking**: which stations are consistently cheapest/most expensive in the 5-mile radius, and by how much.
3. **Spread analysis**: the range (max - min) of local prices over time — is the spread widening or narrowing since the scheme launched?
4. **Brand comparison**: do branded stations (Shell, BP, Esso) consistently charge more than supermarket forecourts (Tesco, Sainsbury's)?
5. **Price change patterns**: how quickly do stations follow each other's price moves? Is there a leader-follower dynamic?

## Conventions

- All prices stored in **pence per litre** (ppl) as floats with one decimal place.
- All timestamps in **UTC ISO 8601** format.
- Config (API credentials, polling interval, centre coordinates, radius) loaded from `.env` via `python-dotenv`.
- Type hints throughout. Use `dataclasses` for domain objects (Station, PriceRecord).
- Keep the poller and analysis code cleanly separated — the poller writes to the DB, analysis code only reads.

## Environment variables

```
FUEL_FINDER_CLIENT_ID=       # OAuth 2.0 client ID from developer portal
FUEL_FINDER_CLIENT_SECRET=   # OAuth 2.0 client secret
FUEL_FINDER_TOKEN_URL=       # Token endpoint (check developer portal docs)
FUEL_FINDER_API_BASE_URL=    # Base URL for price/station endpoints
HASSOCKS_LAT=50.9246
HASSOCKS_LNG=-0.1507
RADIUS_MILES=5
POLL_INTERVAL_MINUTES=30
```

## Getting started

1. Register at https://www.developer.fuel-finder.service.gov.uk/access-latest-fuelprices using GOV.UK One Login.
2. Create an OAuth 2.0 client and note the client ID and secret.
3. Explore the developer portal documentation for endpoint URLs and response schemas — the portal includes a developer kit with endpoint details.
4. Copy `.env.example` to `.env` and fill in credentials.
5. Run `python -m src.poller` to start collecting data.

## Notes

- The Fuel Finder scheme only went live on 2 Feb 2026, so historical data before that date doesn't exist in this source. The project's value grows over time as the dataset accumulates.
- Some stations may have stale prices if they haven't updated — the API provides a `price_last_updated` timestamp. Flag any price older than 7 days as potentially stale in analysis outputs.
- The CSV download (updated twice daily) is a useful sanity check against API results and can serve as a bootstrap to identify all stations in the radius before switching to API polling.
