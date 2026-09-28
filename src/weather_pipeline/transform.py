"""Transformación de respuestas raw a datos diarios normalizados."""

import argparse
import json
import logging
from pathlib import Path

import pandas as pd

from weather_pipeline.extract import write_json

LOGGER = logging.getLogger(__name__)

COLUMN_MAPPING = {
    "temperature_2m_mean": "temperature_mean_c",
    "temperature_2m_max": "temperature_max_c",
    "temperature_2m_min": "temperature_min_c",
    "precipitation_sum": "precipitation_mm",
    "wind_speed_10m_max": "wind_speed_max_kmh",
}


def transform_city(payload, city, thresholds, extraction_id, extracted_at):
    """Función independiente de Airflow, comprobable mediante tests."""
    frame = pd.DataFrame(payload["daily"]).rename(
        columns={"time": "date", **COLUMN_MAPPING}
    )
    frame["date"] = pd.to_datetime(
        frame["date"], format="%Y-%m-%d", errors="raise"
    )

    for column in COLUMN_MAPPING.values():
        # Float64 permite conservar los faltantes como pd.NA.
        frame[column] = pd.to_numeric(
            frame[column], errors="raise"
        ).astype("Float64")

    frame["city"] = city["city"]
    frame["city_name"] = city["city_name"]
    frame["department"] = city["department"]

    # Coordenadas solicitadas y coordenadas de la celda devuelta.
    frame["latitude"] = float(city["latitude"])
    frame["longitude"] = float(city["longitude"])
    frame["source_latitude"] = payload.get("latitude")
    frame["source_longitude"] = payload.get("longitude")

    frame["year"] = frame["date"].dt.year.astype("int16")
    frame["month"] = frame["date"].dt.month.astype("int8")

    frame["temperature_range_c"] = (
        frame["temperature_max_c"] - frame["temperature_min_c"]
    )
    frame["is_rainy_day"] = (
        frame["precipitation_mm"] >= thresholds["rainy_day_mm"]
    )
    frame["is_heavy_rain_day"] = (
        frame["precipitation_mm"] >= thresholds["heavy_rain_day_mm"]
    )
    frame["is_strong_wind_day"] = (
        frame["wind_speed_max_kmh"] >= thresholds["strong_wind_day_kmh"]
    )

    # Lógica nullable: True OR desconocido = True;
    # False OR desconocido = desconocido.
    frame["is_adverse_day"] = (
        frame["is_heavy_rain_day"] | frame["is_strong_wind_day"]
    )
    frame["has_missing_weather"] = (
        frame[list(COLUMN_MAPPING.values())].isna().any(axis=1)
    )

    frame["extraction_id"] = extraction_id
    frame["extracted_at_utc"] = pd.to_datetime(extracted_at, utc=True)
    frame["source"] = "open-meteo"
    frame["timezone"] = payload["timezone"]

    return frame


def transform_weather(manifest_path):
    manifest_path = Path(manifest_path).resolve()
    run_dir = manifest_path.parent
    data_dir = run_dir.parent.parent
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    report_path = (
        data_dir / "quality" / run_dir.name / "raw_quality.json"
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))

    if (
        manifest["status"] != "complete"
        or report["status"] not in ("passed", "passed_with_warnings")
        or report["extraction_id"] != manifest["extraction_id"]
    ):
        raise ValueError("Se requiere una extracción validada.")

    config = manifest["config"]
    thresholds = config["thresholds"]
    for name, value in thresholds.items():
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not 0 < value < float("inf")
        ):
            raise ValueError(f"Umbral inválido: {name}")

    if thresholds["heavy_rain_day_mm"] < thresholds["rainy_day_mm"]:
        raise ValueError("El umbral de lluvia intensa debe ser mayor o igual.")

    frames = []
    for entry in manifest["files"]:
        payload = json.loads(
            (run_dir / entry["raw_path"]).read_text(encoding="utf-8")
        )
        frames.append(transform_city(
            payload=payload,
            city=entry["city"],
            thresholds=thresholds,
            extraction_id=manifest["extraction_id"],
            extracted_at=entry["extracted_at_utc"],
        ))

    daily = pd.concat(frames, ignore_index=True)
    daily = daily.sort_values(["city", "date"]).reset_index(drop=True)

    duplicates = int(daily.duplicated(["city", "date"]).sum())
    if duplicates:
        raise ValueError(f"Se encontraron {duplicates} claves duplicadas.")

    if len(daily) != report["expected_rows"]:
        raise ValueError("El total transformado no coincide con el esperado.")

    if not daily["date"].between(
        pd.Timestamp(config["start_date"]),
        pd.Timestamp(config["end_date"]),
    ).all():
        raise ValueError("Hay fechas fuera del periodo solicitado.")

    # Archivo intermedio; la siguiente tarea publica las particiones.
    staging_dir = data_dir / "staging" / run_dir.name
    staging_dir.mkdir(parents=True, exist_ok=True)
    output_path = staging_dir / "daily.parquet"
    temporary = staging_dir / "daily.parquet.tmp"

    daily.to_parquet(
        temporary, engine="pyarrow", compression="snappy", index=False
    )
    temporary.replace(output_path)

    summary = {
        "extraction_id": manifest["extraction_id"],
        "rows": len(daily),
        "unique_city_date": not bool(duplicates),
        "rows_with_missing_weather": int(
            daily["has_missing_weather"].sum()
        ),
        "thresholds": thresholds,
        "indicator_counts": {
            column: {
                "true": int(daily[column].sum()),
                "unknown": int(daily[column].isna().sum()),
            }
            for column in (
                "is_rainy_day",
                "is_heavy_rain_day",
                "is_strong_wind_day",
                "is_adverse_day",
            )
        },
    }
    write_json(report_path.parent / "transformation_quality.json", summary)

    LOGGER.info("Transformación completa: %s filas.", len(daily))
    LOGGER.info("Indicadores: %s", summary["indicator_counts"])
    LOGGER.info("Archivo intermedio: %s", output_path)
    return str(output_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest_path")
    args = parser.parse_args()
    transform_weather(args.manifest_path)
