"""Pipeline meteorológico: ejecución manual de punta a punta."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="weather_open_meteo",
    description="Datos meteorológicos de cinco ciudades colombianas",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Bogota"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=60),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=15),
    },
    tags=["weather", "open-meteo", "technical-assessment"],
)
def weather_pipeline():
    @task(task_id="extract_weather")
    def extract():
        from weather_pipeline.extract import extract_weather

        return extract_weather()

    @task(task_id="validate_raw_data")
    def validate(manifest_path):
        from weather_pipeline.validate import validate_raw_data

        return validate_raw_data(manifest_path)

    @task(task_id="transform_weather")
    def transform(manifest_path):
        from weather_pipeline.transform import transform_weather

        return transform_weather(manifest_path)

    @task(task_id="load_parquet")
    def load(staging_path):
        from weather_pipeline.load import load_parquet

        return load_parquet(staging_path)

    @task(task_id="generate_dashboard_dataset")
    def dashboard(dataset_dir):
        from weather_pipeline.dashboard import generate_dashboard_dataset

        return generate_dashboard_dataset(dataset_dir)

    raw_manifest = extract()
    validated_manifest = validate(raw_manifest)
    staging_file = transform(validated_manifest)
    published_dataset = load(staging_file)
    dashboard(published_dataset)


weather_pipeline()
