"""Consulta de demostración sobre una extracción publicada."""

import argparse
import json
import os
from pathlib import Path

import duckdb


def select_complete_dataset(data_dir):
    """Selecciona la extracción más reciente con ambas salidas publicadas."""
    candidates = []
    for marker in (data_dir / "processed").glob("*/_SUCCESS.json"):
        dataset = marker.parent
        dashboard_dir = data_dir / "dashboard" / dataset.name
        if all((dashboard_dir / name).is_file() for name in (
            "monthly_weather.parquet",
            "monthly_weather.csv",
            "summary.json",
        )):
            candidates.append(dataset)

    if not candidates:
        raise ValueError("No se encontró una extracción completa.")

    return sorted(candidates, key=lambda path: path.name)[-1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--extraction-id")
    args = parser.parse_args()

    data_dir = Path(os.environ.get("WEATHER_DATA_DIR", "data")).resolve()
    dataset = (
        data_dir / "processed" / args.extraction_id
        if args.extraction_id
        else select_complete_dataset(data_dir)
    )
    publication = json.loads(
        (dataset / "_SUCCESS.json").read_text(encoding="utf-8")
    )
    daily_glob = str(
        dataset / "city=*" / "year=*" / "month=*" / "*.parquet"
    )
    monthly_path = (
        data_dir / "dashboard" / dataset.name / "monthly_weather.parquet"
    )

    print(f"\nExtracción seleccionada: {dataset.name}")
    print(f"Particiones: {publication['partitions']}")

    with duckdb.connect() as connection:
        summary = connection.execute(
            """
            SELECT
                city_name AS ciudad,
                COUNT(*) AS dias,
                ROUND(AVG(temperature_mean_c), 2) AS temperatura_media_c,
                ROUND(SUM(precipitation_mm), 1) AS precipitacion_mm,
                SUM(CAST(is_rainy_day AS INTEGER)) AS dias_lluviosos,
                SUM(CAST(is_adverse_day AS INTEGER)) AS dias_adversos
            FROM read_parquet(?, hive_partitioning = true)
            GROUP BY city_name
            ORDER BY precipitacion_mm DESC
            """,
            [daily_glob],
        ).fetchdf()

        partial = connection.execute(
            """
            SELECT
                city_name AS ciudad,
                month_start AS mes,
                days_observed AS dias_presentes,
                calendar_days AS dias_calendario,
                calendar_coverage_pct AS cobertura_pct
            FROM read_parquet(?)
            WHERE is_partial_month
            ORDER BY city_name
            """,
            [str(monthly_path)],
        ).fetchdf()

    print("\nResumen del periodo por ciudad:")
    print(summary.to_string(index=False))
    print("\nMeses parciales:")
    print(partial.to_string(index=False))


if __name__ == "__main__":
    main()
