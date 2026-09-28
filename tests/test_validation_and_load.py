import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from weather_pipeline.load import load_parquet, inspect_dataset
from weather_pipeline.validate import validate_raw_data


def create_raw_fixture(tmp_path, dates):
    run_dir = tmp_path / "raw" / "extraction_id=test_run"
    city_dir = run_dir / "city=bogota"
    city_dir.mkdir(parents=True)

    city = {
        "city": "bogota",
        "city_name": "Bogotá",
        "department": "Cundinamarca",
        "latitude": 4.711,
        "longitude": -74.0721,
    }
    variables = {
        "temperature_2m_mean": [15.0, 16.0],
        "temperature_2m_max": [20.0, 21.0],
        "temperature_2m_min": [10.0, 11.0],
        "precipitation_sum": [0.0, 20.0],
        "wind_speed_10m_max": [10.0, 15.0],
    }
    payload = {
        "timezone": "America/Bogota",
        "daily": {"time": dates, **variables},
        "daily_units": {
            "time": "iso8601",
            "temperature_2m_mean": "°C",
            "temperature_2m_max": "°C",
            "temperature_2m_min": "°C",
            "precipitation_sum": "mm",
            "wind_speed_10m_max": "km/h",
        },
    }
    content = json.dumps(payload).encode("utf-8")
    raw_path = city_dir / "response.json"
    raw_path.write_bytes(content)

    manifest = {
        "extraction_id": "test_run",
        "status": "complete",
        "config": {
            "start_date": "2026-09-01",
            "end_date": "2026-09-02",
            "timezone": "America/Bogota",
            "temperature_unit": "celsius",
            "precipitation_unit": "mm",
            "wind_speed_unit": "kmh",
            "daily_variables": list(variables),
            "cities": [city],
        },
        "files": [{
            "city": city,
            "raw_path": "city=bogota/response.json",
            "sha256": hashlib.sha256(content).hexdigest(),
        }],
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, raw_path


def test_validation_accepts_complete_dates(tmp_path):
    manifest, _ = create_raw_fixture(
        tmp_path, ["2026-09-01", "2026-09-02"]
    )
    assert validate_raw_data(manifest) == str(manifest.resolve())


@pytest.mark.parametrize("dates", [
    ["2026-09-01", "2026-09-01"],
    ["2026-09-01", "2026-09-03"],
])
def test_validation_rejects_bad_dates(tmp_path, dates):
    manifest, _ = create_raw_fixture(tmp_path, dates)
    with pytest.raises(ValueError, match="Validación fallida"):
        validate_raw_data(manifest)


def test_validation_detects_modified_raw(tmp_path):
    manifest, raw_path = create_raw_fixture(
        tmp_path, ["2026-09-01", "2026-09-02"]
    )
    payload = json.loads(raw_path.read_text())
    payload["daily"]["precipitation_sum"][0] = 99.0
    raw_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="Validación fallida"):
        validate_raw_data(manifest)


def create_staging_fixture(tmp_path):
    staging = (
        tmp_path / "staging" / "extraction_id=test_run" / "daily.parquet"
    )
    staging.parent.mkdir(parents=True)
    frame = pd.DataFrame({
        "city": ["bogota", "bogota"],
        "date": pd.to_datetime(["2026-08-31", "2026-09-01"]),
        "year": [2026, 2026],
        "month": [8, 9],
        "extraction_id": ["test_run", "test_run"],
        "precipitation_mm": [0.0, 20.0],
    })
    frame.to_parquet(staging, index=False)
    return staging


def test_load_retry_does_not_duplicate_data(tmp_path):
    staging = create_staging_fixture(tmp_path)
    first = load_parquet(staging)
    second = load_parquet(staging)

    assert first == second
    output = Path(first)
    assert len(list(output.rglob("*.parquet"))) == 2
    verification = inspect_dataset(output)
    assert verification["rows"] == 2
    assert verification["unique_keys"] == 2


def test_load_rejects_changed_input_for_published_run(tmp_path):
    staging = create_staging_fixture(tmp_path)
    load_parquet(staging)

    modified = pd.read_parquet(staging)
    modified.loc[0, "precipitation_mm"] = 5.0
    modified.to_parquet(staging, index=False)

    with pytest.raises(ValueError, match="La entrada cambió"):
        load_parquet(staging)
