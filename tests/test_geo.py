from __future__ import annotations

from src.geo import filter_stations_by_radius, haversine_miles

HASSOCKS = (50.9246, -0.1507)


def test_same_point_returns_zero():
    assert haversine_miles(*HASSOCKS, *HASSOCKS) == 0.0


def test_hassocks_to_burgess_hill():
    # Burgess Hill is ~2 miles north of Hassocks
    d = haversine_miles(*HASSOCKS, 50.9530, -0.1290)
    assert 1.5 < d < 3.0


def test_hassocks_to_brighton():
    # Brighton is ~7-8 miles south
    d = haversine_miles(*HASSOCKS, 50.8225, -0.1372)
    assert 6.0 < d < 9.0


def test_hassocks_to_london_beyond_radius():
    # London is ~45 miles
    d = haversine_miles(*HASSOCKS, 51.5074, -0.1278)
    assert d > 30


def test_filter_stations_by_radius():
    stations = [
        {"name": "Near", "latitude": 50.9300, "longitude": -0.1500},
        {"name": "Far", "latitude": 51.5074, "longitude": -0.1278},
        {"name": "Edge", "latitude": 50.8500, "longitude": -0.1500},
    ]
    result = filter_stations_by_radius(stations, *HASSOCKS, 5.0)
    names = [s["name"] for s in result]
    assert "Near" in names
    assert "Far" not in names
    # All results should have distance_miles
    for s in result:
        assert "distance_miles" in s
    # Should be sorted by distance
    distances = [s["distance_miles"] for s in result]
    assert distances == sorted(distances)
