import pandas as pd
import pytest

from weather_pipeline.transform import transform_city
from weather_pipeline.dashboard import summarize_monthly


CITY = {
    "city": "bogota",
    "city_name": "Bogotá",
    "department": "Cundinamarca",
    "latitude": 4.7110,
    "longitude": -74.0721,
}
THRESHOLDS = {
    "rainy_day_mm": 1.0,
    "heavy_rain_day_mm": 20.0,
    "strong_wind_day_kmh": 40.0,
}


def make_daily(rain, wind, dates=None):
    n = len(rain)
    payload = {
        "latitude": 4.7,
        "longitude": -74.1,
        "timezone": "America/Bogota",
        "daily": {
            "time": dates or [
                f"2026-09-{i + 1:02d}" for i in range(n)
            ],
            "temperature_2m_mean": [15.0] * n,
            "temperature_2m_max": [20.0] * n,
            "temperature_2m_min": [10.0] * n,
            "precipitation_sum": rain,
            "wind_speed_10m_max": wind,
        },
    }
    return transform_city(
        payload, CITY, THRESHOLDS, "test_run",
        "2026-09-27T23:00:00+00:00",
    )


def test_indicator_boundaries():
    daily = make_daily(
        [0.9, 1.0, 19.9, 20.0],
        [39.9, 40.0, 0.0, 0.0],
    )
    assert daily["is_rainy_day"].tolist() == [False, True, True, True]
    assert daily["is_heavy_rain_day"].tolist() == [False, False, False, True]
    assert daily["is_strong_wind_day"].tolist() == [False, True, False, False]
    assert daily["is_adverse_day"].tolist() == [False, True, False, True]
    assert daily["temperature_range_c"].tolist() == [10.0] * 4


def test_missing_values_remain_unknown():
    daily = make_daily(
        [None, None, 20.0, 0.0],
        [0.0, 40.0, None, None],
    )
    assert pd.isna(daily.loc[0, "is_rainy_day"])
    assert pd.isna(daily.loc[0, "is_adverse_day"])
    assert bool(daily.loc[1, "is_adverse_day"])
    assert bool(daily.loc[2, "is_adverse_day"])
    assert pd.isna(daily.loc[3, "is_adverse_day"])
    assert daily["has_missing_weather"].all()


def test_monthly_metrics_use_known_days():
    daily = make_daily([None, 0.0, 20.0], [0.0, 0.0, 0.0])
    row = summarize_monthly(daily).iloc[0]

    assert row["days_observed"] == 3
    assert row["precipitation_mm_valid_days"] == 2
    assert row["precipitation_total_mm"] == pytest.approx(20.0)
    assert row["precipitation_mean_daily_mm"] == pytest.approx(10.0)
    assert row["rainy_days"] == 1
    assert row["rainy_days_valid_days"] == 2
    assert row["rainy_days_pct"] == pytest.approx(50.0)


def test_all_missing_rain_is_not_zero():
    daily = make_daily([None, None], [0.0, 0.0])
    row = summarize_monthly(daily).iloc[0]

    assert pd.isna(row["precipitation_total_mm"])
    assert pd.isna(row["rainy_days"])
    assert pd.isna(row["rainy_days_pct"])
    assert row["rainy_days_valid_days"] == 0


def test_september_is_partial():
    daily = make_daily([0.0] * 15, [0.0] * 15)
    row = summarize_monthly(daily).iloc[0]

    assert row["days_observed"] == 15
    assert row["calendar_days"] == 30
    assert bool(row["is_partial_month"])
    assert row["calendar_coverage_pct"] == pytest.approx(50.0)


def test_complete_february():
    dates = pd.date_range("2026-02-01", "2026-02-28").strftime(
        "%Y-%m-%d"
    ).tolist()
    daily = make_daily([0.0] * 28, [0.0] * 28, dates)
    row = summarize_monthly(daily).iloc[0]

    assert row["calendar_days"] == 28
    assert not bool(row["is_partial_month"])
    assert row["calendar_coverage_pct"] == pytest.approx(100.0)
