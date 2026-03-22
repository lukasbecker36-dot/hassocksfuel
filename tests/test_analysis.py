from __future__ import annotations

from src.analysis import brand_comparison, spread_analysis, station_ranking


def test_station_ranking(populated_conn):
    df = station_ranking(populated_conn, fuel_type="B7", days=30)
    assert not df.empty
    # S1 (Tesco) should be cheapest on average
    assert df.iloc[0]["name"] == "Tesco Burgess Hill"
    assert "rank" in df.columns


def test_spread_analysis(populated_conn):
    df = spread_analysis(populated_conn, fuel_type="B7", days=30)
    assert not df.empty
    assert "spread" in df.columns
    # All spreads should be non-negative
    assert (df["spread"] >= 0).all()


def test_brand_comparison(populated_conn):
    df = brand_comparison(populated_conn, fuel_type="B7", days=30)
    assert not df.empty
    assert "premium_vs_cheapest" in df.columns
    # The cheapest brand should have premium_vs_cheapest == 0
    assert df["premium_vs_cheapest"].min() == 0
