"""Generación del resumen mensual para consumo analítico."""

import argparse
import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

from weather_pipeline.extract import write_json

LOGGER = logging.getLogger(__name__)

FLAGS = {
    "is_rainy_day": "rainy_days",
    "is_heavy_rain_day": "heavy_rain_days",
    "is_strong_wind_day": "strong_wind_days",
    "is_adverse_day": "adverse_days",
}


def summarize_monthly(daily):
    """Agrega datos sin convertir valores desconocidos en ceros."""
    daily = daily.copy()
    daily["date"] = pd.to_datetime(daily["date"])
    daily["month_start"] = (
        daily["date"].dt.to_period("M").dt.to_timestamp()
    )

    rows = []
    for (city, month_start), group in daily.groupby(
        ["city", "month_start"], observed=True, sort=True
    ):
        days_observed = group["date"].nunique()
        calendar_days = month_start.days_in_month

        row = {
            "city": city,
            "city_name": group["city_name"].iloc[0],
            "department": group["department"].iloc[0],
            "latitude": group["latitude"].iloc[0],
            "longitude": group["longitude"].iloc[0],
            "month_start": month_start,
            "year": month_start.year,
            "month": month_start.month,
            "first_observed_date": group["date"].min(),
            "last_observed_date": group["date"].max(),
            "days_observed": days_observed,
            "calendar_days": calendar_days,
            "calendar_coverage_pct": 100 * days_observed / calendar_days,
            "is_partial_month": days_observed < calendar_days,
            "temperature_mean_c": group["temperature_mean_c"].mean(),
            "temperature_max_c": group["temperature_max_c"].max(),
            "temperature_min_c": group["temperature_min_c"].min(),
            "temperature_range_mean_c": group["temperature_range_c"].mean(),
            "precipitation_total_mm": group["precipitation_mm"].sum(
                min_count=1
            ),
            "precipitation_mean_daily_mm": group["precipitation_mm"].mean(),
            "wind_speed_max_kmh": group["wind_speed_max_kmh"].max(),
            "days_with_missing_weather": int(
                group["has_missing_weather"].sum()
            ),
            "extraction_id": group["extraction_id"].iloc[0],
        }

        # Denominadores para interpretar métricas con valores faltantes.
        for column in (
            "temperature_mean_c",
            "temperature_max_c",
            "temperature_min_c",
            "precipitation_mm",
            "wind_speed_max_kmh",
        ):
            row[f"{column}_valid_days"] = int(group[column].count())

        for flag, metric in FLAGS.items():
            valid_days = int(group[flag].count())
            event_days = (
                int(group[flag].sum()) if valid_days else None
            )
            row[metric] = event_days
            row[f"{metric}_valid_days"] = valid_days
            row[f"{metric}_pct"] = (
                100 * event_days / valid_days if valid_days else None
            )

        rows.append(row)

    return pd.DataFrame(rows)


def generate_dashboard_dataset(dataset_dir):
    dataset_dir = Path(dataset_dir).resolve()
    success_path = dataset_dir / "_SUCCESS.json"
    publication = json.loads(success_path.read_text(encoding="utf-8"))

    parquet_glob = str(
        dataset_dir / "city=*" / "year=*" / "month=*" / "*.parquet"
    )
    with duckdb.connect() as connection:
        daily = connection.execute(
            """
            SELECT *
            FROM read_parquet(?, hive_partitioning = true)
            ORDER BY city, date
            """,
            [parquet_glob],
        ).fetchdf()

    if (
        len(daily) != publication["verification"]["rows"]
        or daily.duplicated(["city", "date"]).any()
    ):
        raise ValueError("El dataset diario no coincide con la publicación.")

    extraction_id = publication["extraction_id"]
    if (
        daily["extraction_id"].isna().any()
        or set(daily["extraction_id"]) != {extraction_id}
    ):
        raise ValueError("El dataset mezcla extracciones.")

    monthly = summarize_monthly(daily)
    if monthly.duplicated(["city", "month_start"]).any():
        raise ValueError("Hay claves ciudad-mes duplicadas.")

    # Comprueba que las sumas mensuales conservan los conteos diarios.
    for flag, metric in FLAGS.items():
        if int(monthly[metric].sum()) != int(daily[flag].sum()):
            raise ValueError(f"El conteo mensual no coincide: {metric}")

    data_dir = dataset_dir.parent.parent
    output_dir = data_dir / "dashboard" / extraction_id
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / "monthly_weather.parquet"
    temporary_parquet = output_dir / "monthly_weather.parquet.tmp"
    monthly.to_parquet(
        temporary_parquet,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )
    temporary_parquet.replace(parquet_path)

    csv_path = output_dir / "monthly_weather.csv"
    temporary_csv = output_dir / "monthly_weather.csv.tmp"
    monthly.to_csv(
        temporary_csv, index=False, encoding="utf-8-sig"
    )
    temporary_csv.replace(csv_path)

    write_json(output_dir / "summary.json", {
        "extraction_id": extraction_id,
        "rows": len(monthly),
        "cities": int(monthly["city"].nunique()),
        "months": int(monthly["month_start"].nunique()),
        "partial_city_months": int(monthly["is_partial_month"].sum()),
        "source_dataset": str(dataset_dir),
        "notes": [
            "Los totales corresponden a los días con datos disponibles.",
            "Los porcentajes de eventos usan días con indicador conocido.",
            "La cobertura calendario mide fechas, no completitud de variables.",
            "No se extrapolan los totales de meses incompletos.",
        ],
    })

    LOGGER.info(
        "Resumen generado: %s filas; %s ciudades; %s meses.",
        len(monthly),
        monthly["city"].nunique(),
        monthly["month_start"].nunique(),
    )
    LOGGER.info(
        "Registros ciudad-mes incompletos: %s",
        monthly["is_partial_month"].sum(),
    )
    LOGGER.info("Parquet: %s", parquet_path)
    LOGGER.info("CSV: %s", csv_path)

    print(monthly[[
        "city", "month_start", "days_observed",
        "precipitation_total_mm", "adverse_days",
    ]].tail(9).to_string(index=False))

    return str(parquet_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_dir")
    args = parser.parse_args()
    generate_dashboard_dataset(args.dataset_dir)
