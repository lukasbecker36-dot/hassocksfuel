from __future__ import annotations

import math

EARTH_RADIUS_MILES = 3958.8


def haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance between two points in miles."""
    lat1, lng1, lat2, lng2 = map(math.radians, (lat1, lng1, lat2, lng2))
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    return 2 * EARTH_RADIUS_MILES * math.asin(math.sqrt(a))


def filter_stations_by_radius(
    stations: list[dict],
    centre_lat: float,
    centre_lng: float,
    radius_miles: float,
    *,
    lat_key: str = "latitude",
    lng_key: str = "longitude",
) -> list[dict]:
    """Filter station dicts by distance, adding a 'distance_miles' field.

    Returns stations within radius, sorted by distance ascending.
    """
    results = []
    for s in stations:
        d = haversine_miles(centre_lat, centre_lng, float(s[lat_key]), float(s[lng_key]))
        if d <= radius_miles:
            results.append({**s, "distance_miles": round(d, 2)})
    results.sort(key=lambda x: x["distance_miles"])
    return results
