from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Station:
    station_id: str
    name: str
    lat: float
    lng: float
    distance_miles: float
    brand: str | None = None
    operator: str | None = None
    address: str | None = None
    postcode: str | None = None


@dataclass(frozen=True)
class PriceRecord:
    station_id: str
    fuel_type: str  # "E10" or "B7"
    price_ppl: float  # pence per litre
    price_updated_at: str  # ISO 8601 UTC
    fetched_at: str  # ISO 8601 UTC


@dataclass(frozen=True)
class Settings:
    client_id: str = field(default_factory=lambda: os.environ.get("FUEL_FINDER_CLIENT_ID", ""))
    client_secret: str = field(default_factory=lambda: os.environ.get("FUEL_FINDER_CLIENT_SECRET", ""))
    token_url: str = field(
        default_factory=lambda: os.environ.get(
            "FUEL_FINDER_TOKEN_URL",
            "https://auth.fuelfinder.service.gov.uk/oauth2/token",
        )
    )
    api_base_url: str = field(
        default_factory=lambda: os.environ.get(
            "FUEL_FINDER_API_BASE_URL",
            "https://api.fuelfinder.service.gov.uk/v1",
        )
    )
    hassocks_lat: float = field(
        default_factory=lambda: float(os.environ.get("HASSOCKS_LAT", "50.9246"))
    )
    hassocks_lng: float = field(
        default_factory=lambda: float(os.environ.get("HASSOCKS_LNG", "-0.1507"))
    )
    radius_miles: float = field(
        default_factory=lambda: float(os.environ.get("RADIUS_MILES", "5"))
    )
    poll_interval_minutes: int = field(
        default_factory=lambda: int(os.environ.get("POLL_INTERVAL_MINUTES", "30"))
    )
    db_path: str = field(
        default_factory=lambda: os.environ.get(
            "DB_PATH", str(PROJECT_ROOT / "data" / "fuel_prices.db")
        )
    )


settings = Settings()
