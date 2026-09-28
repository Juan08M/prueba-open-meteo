"""Validación de respuestas raw y generación del reporte de calidad."""

import argparse
import hashlib
import json
import logging
import math
from datetime import date, timedelta
from pathlib import Path

from weather_pipeline.extract import write_json

LOGGER = logging.getLogger(__name__)

EXPECTED_UNITS = {
    "temperature_2m_mean": "°C",
    "temperature_2m_max": "°C",
    "temperature_2m_min": "°C",
    "precipitation_sum": "mm",
    "wind_speed_10m_max": "km/h",
}


def validate_raw_data(manifest_path):
    manifest_path = Path(manifest_path).resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_dir = manifest_path.parent
    config = manifest["config"]

    if manifest["status"] != "complete":
        raise ValueError("No se puede validar una extracción incompleta.")

    start = date.fromisoformat(config["start_date"])
    end = date.fromisoformat(config["end_date"])
    expected_dates = [
        (start + timedelta(days=i)).isoformat()
        for i in range((end - start).days + 1)
    ]
    expected_cities = {city["city"] for city in config["cities"]}
    received_cities = [
        entry["city"]["city"] for entry in manifest["files"]
    ]

    errors = []
    if (
        set(received_cities) != expected_cities
        or len(received_cities) != len(expected_cities)
    ):
        errors.append("Las ciudades recibidas no coinciden con la configuración.")

    # Las reglas y nombres de indicadores usan estas unidades.
    if (
        config["temperature_unit"] != "celsius"
        or config["precipitation_unit"] != "mm"
        or config["wind_speed_unit"] != "kmh"
    ):
        errors.append("Esta transformación requiere Celsius, mm y km/h.")

    report = {
        "extraction_id": manifest["extraction_id"],
        "expected_rows": len(expected_dates) * len(expected_cities),
        "received_rows": 0,
        "total_missing_values": 0,
        "cities": [],
        "errors": errors,
    }

    for entry in manifest["files"]:
        city = entry["city"]["city"]
        city_errors = []
        missing = {}
        raw_path = (run_dir / entry["raw_path"]).resolve()

        if not raw_path.is_relative_to(run_dir):
            raise ValueError("El archivo raw está fuera de la extracción.")

        content = raw_path.read_bytes()
        if hashlib.sha256(content).hexdigest() != entry["sha256"]:
            city_errors.append("La huella SHA-256 no coincide.")

        payload = json.loads(content)
        daily = payload.get("daily", {})
        units = payload.get("daily_units", {})
        dates = daily.get("time", [])

        if not isinstance(dates, list):
            city_errors.append("El campo time no es una lista.")
            dates = []

        duplicates = len(dates) - len(set(map(str, dates)))
        if duplicates:
            city_errors.append(f"Fechas duplicadas: {duplicates}.")

        if dates != expected_dates:
            city_errors.append(
                "Las fechas no coinciden con el periodo y orden esperados."
            )

        if payload.get("timezone") != config["timezone"]:
            city_errors.append("Zona horaria inesperada.")

        if units.get("time") != "iso8601":
            city_errors.append("Formato de fecha inesperado.")

        valid_columns = {}
        for variable in config["daily_variables"]:
            values = daily.get(variable)
            if not isinstance(values, list):
                city_errors.append(f"Variable ausente o inválida: {variable}.")
                continue

            if len(values) != len(dates):
                city_errors.append(f"Longitud incorrecta: {variable}.")
                continue

            if units.get(variable) != EXPECTED_UNITS.get(variable):
                city_errors.append(f"Unidad inesperada: {variable}.")

            missing[variable] = sum(value is None for value in values)
            invalid = [
                value for value in values
                if value is not None and (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                )
            ]
            if invalid:
                city_errors.append(f"Valores no numéricos o no finitos: {variable}.")
                continue

            valid_columns[variable] = values

            if variable in ("precipitation_sum", "wind_speed_10m_max"):
                if any(value is not None and value < 0 for value in values):
                    city_errors.append(f"Valores negativos: {variable}.")

        temperature_columns = [
            "temperature_2m_min",
            "temperature_2m_mean",
            "temperature_2m_max",
        ]
        if all(name in valid_columns for name in temperature_columns):
            inconsistent = 0
            for low, mean, high in zip(
                *(valid_columns[name] for name in temperature_columns)
            ):
                if (
                    (low is not None and high is not None and low > high)
                    or (low is not None and mean is not None and low > mean)
                    or (mean is not None and high is not None and mean > high)
                ):
                    inconsistent += 1
            if inconsistent:
                city_errors.append(
                    f"Temperaturas mínimas/medias/máximas incoherentes: "
                    f"{inconsistent}."
                )

        report["received_rows"] += len(dates)
        report["total_missing_values"] += sum(missing.values())
        report["cities"].append({
            "city": city,
            "rows": len(dates),
            "duplicate_dates": duplicates,
            "missing_by_variable": missing,
            "errors": city_errors,
        })
        errors.extend(f"{city}: {error}" for error in city_errors)

        LOGGER.info(
            "%s: %s filas, %s faltantes, %s errores",
            city, len(dates), sum(missing.values()), len(city_errors),
        )

    report["status"] = (
        "failed" if errors
        else "passed_with_warnings" if report["total_missing_values"]
        else "passed"
    )

    data_dir = run_dir.parent.parent
    quality_dir = data_dir / "quality" / run_dir.name
    quality_dir.mkdir(parents=True, exist_ok=True)
    report_path = quality_dir / "raw_quality.json"
    write_json(report_path, report)

    LOGGER.info("Reporte de calidad: %s", report_path)

    if errors:
        raise ValueError(
            f"Validación fallida con {len(errors)} errores. "
            f"Consulta {report_path}"
        )

    LOGGER.info(
        "Validación aprobada: %s filas; %s valores faltantes.",
        report["received_rows"], report["total_missing_values"],
    )
    return str(manifest_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest_path")
    args = parser.parse_args()
    validate_raw_data(args.manifest_path)
