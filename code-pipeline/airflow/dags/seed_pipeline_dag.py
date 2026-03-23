import json
import re
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from worker.seed.services.seed_ingest_service import run_curated_repo_ingestion, run_seed_ingestion
from worker.seed.services.seed_normalize_service import run_seed_normalization
from worker.seed.services.seed_qualify_service import run_seed_qualification


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


SEED_CONFIG_PATH = Path("/opt/airflow/config/seed_packages.json")
CURATED_REPO_LIST_CONFIG_PATH = Path("/opt/airflow/config/curated_repo_lists.json")


def load_seed_packages(registry: str) -> list[str]:
    payload = json.loads(SEED_CONFIG_PATH.read_text(encoding="utf-8"))
    package_names = payload.get(registry, [])
    return [name for name in package_names if isinstance(name, str) and name.strip()]


def load_curated_repo_lists() -> dict[str, list[str | dict]]:
    payload = json.loads(CURATED_REPO_LIST_CONFIG_PATH.read_text(encoding="utf-8"))
    curated_lists = {}

    for list_name, repo_entries in payload.items():
        if not isinstance(list_name, str) or not isinstance(repo_entries, list):
            continue

        curated_lists[list_name] = [
            entry for entry in repo_entries if isinstance(entry, (str, dict))
        ]

    return curated_lists


def slugify_task_suffix(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")


with DAG(
    dag_id="seed_pipeline_dag",
    default_args=default_args,
    description="Seed discovery pipeline",
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

    curated_ingest_tasks = []
    for list_name, repo_entries in load_curated_repo_lists().items():
        curated_ingest_tasks.append(
            PythonOperator(
                task_id=f"seed_ingestion_curated_{slugify_task_suffix(list_name)}",
                python_callable=run_curated_repo_ingestion,
                op_kwargs={
                    "list_name": list_name,
                    "repo_entries": repo_entries,
                },
            )
        )

    normalize_task = PythonOperator(
        task_id="seed_normalization",
        python_callable=run_seed_normalization,
    )

    qualify_task = PythonOperator(
        task_id="seed_qualification",
        python_callable=run_seed_qualification,
    )

    [ingest_pypi_task, ingest_npm_task, *curated_ingest_tasks] >> normalize_task >> qualify_task
