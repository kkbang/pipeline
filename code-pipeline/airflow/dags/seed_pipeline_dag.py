import json
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from worker.seed.services.seed_ingest_service import run_seed_ingestion
from worker.seed.services.seed_normalize_service import run_seed_normalization
from worker.seed.services.seed_qualify_service import run_seed_qualification


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


SEED_CONFIG_PATH = Path("/opt/airflow/config/seed_packages.json")


def load_seed_packages(registry: str) -> list[str]:
    payload = json.loads(SEED_CONFIG_PATH.read_text(encoding="utf-8"))
    package_names = payload.get(registry, [])
    return [name for name in package_names if isinstance(name, str) and name.strip()]


with DAG(
    dag_id="seed_pipeline_dag",
    default_args=default_args,
    description="Package registry based seed pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["seed", "package-registry"],
) as dag:

    ingest_pypi_task = PythonOperator(
        task_id="seed_ingestion_pypi",
        python_callable=run_seed_ingestion,
        op_kwargs={
            "registry": "pypi",
            "package_names": load_seed_packages("pypi"),
        },
    )

    ingest_npm_task = PythonOperator(
        task_id="seed_ingestion_npm",
        python_callable=run_seed_ingestion,
        op_kwargs={
            "registry": "npm",
            "package_names": load_seed_packages("npm"),
        },
    )

    normalize_task = PythonOperator(
        task_id="seed_normalization",
        python_callable=run_seed_normalization,
    )

    qualify_task = PythonOperator(
        task_id="seed_qualification",
        python_callable=run_seed_qualification,
    )

    [ingest_pypi_task, ingest_npm_task] >> normalize_task >> qualify_task
