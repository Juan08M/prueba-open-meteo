"""Extracción parametrizable de datos históricos de Open-Meteo."""

import hashlib
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)


def write_json(path, content):
    """Publica el archivo solo cuando la escritura está completa."""
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(content, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def extract_weather(config_path=None, data_dir=None):
    """Descarga las ciudades y devuelve la ruta del manifiesto completo."""
    config_path = Path(
        config_path
        or os.environ.get("WEATHER_CONFIG_PATH", "config/settings.json")
    )
    data_dir = Path(
        data_dir or os.environ.get("WEATHER_DATA_DIR", "data")
    ).resolve()

    config = json.loads(config_path.read_text(encoding="utf-8"))

    start = date.fromisoformat(config["start_date"])
    end = date.fromisoformat(config["end_date"])
    if start > end:
        raise ValueError("La fecha inicial es posterior a la final.")

    cities = config["cities"]
    identifiers = [city["city"] for city in cities]
    if not cities or len(identifiers) != len(set(identifiers)):
        raise ValueError("Debe haber ciudades con identificadores únicos.")

    for identifier in identifiers:
        if not identifier or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789_-"
            for character in identifier
        ):
            raise ValueError(f"Identificador de ciudad inválido: {identifier}")

    extraction_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "_"
        + uuid4().hex[:8]
    )
    run_dir = data_dir / "raw" / f"extraction_id={extraction_id}"
    run_dir.mkdir(parents=True, exist_ok=False)

    manifest = {
        "extraction_id": extraction_id,
        "status": "running",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "files": [],
    }
    manifest_path = run_dir / "manifest.json"
    write_json(manifest_path, manifest)

    retry_policy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    try:
        with requests.Session() as session:
            session.mount(
                "https://",
                HTTPAdapter(max_retries=retry_policy),
            )
            session.headers.update({
                "User-Agent": "weather-engineering-assessment/1.0"
            })

            for city in cities:
                params = {
                    "latitude": city["latitude"],
                    "longitude": city["longitude"],
                    "start_date": config["start_date"],
                    "end_date": config["end_date"],
                    "timezone": config["timezone"],
                    "daily": ",".join(config["daily_variables"]),
                    "temperature_unit": config["temperature_unit"],
                    "wind_speed_unit": config["wind_speed_unit"],
                    "precipitation_unit": config["precipitation_unit"],
                }

                LOGGER.info("Consultando %s", city["city_name"])
                response = session.get(
                    config["api_url"],
                    params=params,
                    timeout=(10, 90),
                )
                response.raise_for_status()

                city_dir = (
                    run_dir
                    / f"city={city['city']}"
                    / f"period={start}_{end}"
                )
                city_dir.mkdir(parents=True, exist_ok=False)

                # Conservamos el cuerpo original antes de interpretarlo.
                raw_path = city_dir / "response.json"
                temporary = raw_path.with_suffix(".json.tmp")
                temporary.write_bytes(response.content)
                temporary.replace(raw_path)

                metadata = {
                    "city": city,
                    "extracted_at_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "request_url": response.url,
                    "http_status": response.status_code,
                    "sha256": hashlib.sha256(
                        response.content
                    ).hexdigest(),
                    "raw_path": str(raw_path.relative_to(run_dir)),
                }
                write_json(city_dir / "metadata.json", metadata)

                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError(
                        f"Respuesta inesperada para {city['city']}"
                    )
                if payload.get("error"):
                    raise ValueError(
                        f"Error API: {payload.get('reason')}"
                    )
                if not isinstance(payload.get("daily"), dict):
                    raise ValueError(
                        f"Respuesta sin datos diarios: {city['city']}"
                    )

                manifest["files"].append(metadata)
                write_json(manifest_path, manifest)
                LOGGER.info(
                    "Respuesta guardada: %s; fechas recibidas: %s",
                    city["city"],
                    len(payload["daily"].get("time", [])),
                )

        manifest["status"] = "complete"
        manifest["finished_at_utc"] = datetime.now(
            timezone.utc
        ).isoformat()
        write_json(manifest_path, manifest)

    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        write_json(manifest_path, manifest)
        LOGGER.exception(
            "Extracción fallida. Evidencia disponible en %s", run_dir
        )
        raise

    LOGGER.info("Extracción completa: %s", manifest_path)
    return str(manifest_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    print(extract_weather())
