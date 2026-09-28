"""Publicación de Parquet particionado y verificación con DuckDB."""

import argparse
import hashlib
import json
import logging
import shutil
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd

from weather_pipeline.extract import write_json

LOGGER = logging.getLogger(__name__)


def inspect_dataset(directory):
    parquet_glob = str(directory / "city=*" / "year=*" / "month=*" / "*.parquet")
    with duckdb.connect() as connection:
        result = connection.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT (city, date)) AS unique_keys,
                COUNT(DISTINCT city) AS cities,
                MIN(date) AS first_date,
                MAX(date) AS last_date
            FROM read_parquet(?, hive_partitioning = true)
            """,
            [parquet_glob],
        ).fetchone()

    return {
        "rows": result[0],
        "unique_keys": result[1],
        "cities": result[2],
        "first_date": str(result[3]),
        "last_date": str(result[4]),
    }


def load_parquet(staging_path):
    staging_path = Path(staging_path).resolve()
    daily = pd.read_parquet(staging_path)

    if daily.empty:
        raise ValueError("El archivo intermedio está vacío.")

    extraction_ids = daily["extraction_id"].dropna().unique()
    if len(extraction_ids) != 1 or daily["extraction_id"].isna().any():
        raise ValueError("Debe existir exactamente una extracción.")

    extraction_id = str(extraction_ids[0])
    if any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        for character in extraction_id
    ):
        raise ValueError("Identificador de extracción inválido.")

    if daily.duplicated(["city", "date"]).any():
        raise ValueError("Hay claves ciudad-fecha duplicadas.")

    data_dir = staging_path.parent.parent.parent
    output_dir = data_dir / "processed" / extraction_id
    fingerprint = hashlib.sha256(staging_path.read_bytes()).hexdigest()
    success_path = output_dir / "_SUCCESS.json"

    # Un reintento sobre la misma entrada reutiliza el dataset publicado.
    if output_dir.exists():
        if not success_path.exists():
            raise ValueError("El destino existe sin marcador de publicación.")
        previous = json.loads(success_path.read_text(encoding="utf-8"))
        if previous["input_sha256"] != fingerprint:
            raise ValueError(
                "La entrada cambió para una extracción ya publicada."
            )
        actual = inspect_dataset(output_dir)
        if actual != previous["verification"]:
            raise ValueError("El dataset publicado no coincide con su registro.")
        LOGGER.info("Dataset ya publicado y verificado: %s", output_dir)
        return str(output_dir)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_dir.parent / f".tmp-{uuid4().hex}"
    temporary_dir.mkdir()

    try:
        partitions = 0
        for (city, year, month), group in daily.groupby(
            ["city", "year", "month"], observed=True
        ):
            partition_dir = (
                temporary_dir
                / f"city={city}"
                / f"year={int(year)}"
                / f"month={int(month):02d}"
            )
            partition_dir.mkdir(parents=True)

            # Las claves de partición se recuperan desde las carpetas.
            group.drop(columns=["city", "year", "month"]).to_parquet(
                partition_dir / "part-00000.parquet",
                engine="pyarrow",
                compression="snappy",
                index=False,
            )
            partitions += 1

        verification = inspect_dataset(temporary_dir)
        if (
            verification["rows"] != len(daily)
            or verification["unique_keys"] != len(daily)
            or verification["cities"] != daily["city"].nunique()
        ):
            raise ValueError("La verificación de lectura Parquet falló.")

        write_json(temporary_dir / "_SUCCESS.json", {
            "extraction_id": extraction_id,
            "input_sha256": fingerprint,
            "partition_columns": ["city", "year", "month"],
            "partitions": partitions,
            "verification": verification,
        })

        # Publica la carpeta completa en el mismo sistema de archivos.
        temporary_dir.rename(output_dir)

    except Exception:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise

    LOGGER.info("Publicación completa: %s particiones.", partitions)
    LOGGER.info("Verificación DuckDB: %s", verification)
    LOGGER.info("Dataset publicado: %s", output_dir)
    return str(output_dir)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("staging_path")
    args = parser.parse_args()
    load_parquet(args.staging_path)
