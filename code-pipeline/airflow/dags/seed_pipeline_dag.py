import json
import re
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from worker.seed.services.benchmark_artifact_fetch_service import run_benchmark_artifact_fetch
from worker.seed.services.seed_ingest_service import (
    run_benchmark_dataset_ingestion,
    run_curated_repo_ingestion,
    run_seed_ingestion,
)
from worker.seed.services.seed_normalize_service import run_seed_normalization
from worker.seed.services.seed_qualify_service import run_seed_qualification


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


SEED_CONFIG_PATH = Path("/opt/airflow/config/seed_packages.json")
CURATED_REPO_LIST_CONFIG_PATH = Path("/opt/airflow/config/curated_repo_lists.json")
BENCHMARK_DATASET_CONFIG_PATH = Path("/opt/airflow/config/benchmark_datasets.json")


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


def load_benchmark_datasets() -> dict[str, dict]:
    payload = json.loads(BENCHMARK_DATASET_CONFIG_PATH.read_text(encoding="utf-8"))
    benchmark_datasets = {}

    for dataset_name, dataset_config in payload.items():
        if not isinstance(dataset_name, str) or not isinstance(dataset_config, dict):
            continue

        if dataset_config.get("enabled") is not True:
            continue

        benchmark_datasets[dataset_name] = dataset_config

    return benchmark_datasets


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

    package_registry_ingest_tasks = []
    for registry in ["pypi", "npm"]:
        package_registry_ingest_tasks.append(
            PythonOperator(
                task_id=f"seed_ingestion_{registry}",
                python_callable=run_seed_ingestion,
                op_kwargs={
                    "registry": registry,
                    "package_names": load_seed_packages(registry),
                },
            )
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

    benchmark_ingest_tasks = []
    for dataset_name, dataset_config in load_benchmark_datasets().items():
        ingest_task = PythonOperator(
            task_id=f"seed_ingestion_benchmark_{slugify_task_suffix(dataset_name)}",
            python_callable=run_benchmark_dataset_ingestion,
            op_kwargs={
                "dataset_name": dataset_name,
                "dataset_config": dataset_config,
            },
        )
        benchmark_ingest_tasks.append(ingest_task)

        artifact_fetch = dataset_config.get("artifact_fetch") or {}
        if artifact_fetch.get("enabled") is True:
            fetch_task = PythonOperator(
                task_id=f"benchmark_artifact_fetch_{slugify_task_suffix(dataset_name)}",
                python_callable=run_benchmark_artifact_fetch,
                op_kwargs={
                    "dataset_name": dataset_name,
                    "dataset_config": dataset_config,
                },
            )
            fetch_task >> ingest_task

    normalize_task = PythonOperator(
        task_id="seed_normalization",
        python_callable=run_seed_normalization,
    )

    qualify_task = PythonOperator(
        task_id="seed_qualification",
        python_callable=run_seed_qualification,
    )

    [*package_registry_ingest_tasks, *curated_ingest_tasks, *benchmark_ingest_tasks] >> normalize_task >> qualify_task


