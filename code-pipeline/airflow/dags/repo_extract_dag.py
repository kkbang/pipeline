from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from worker.common.config import settings
from worker.repo.repo_file_extract_service import run_repo_file_extraction_for_repo
from worker.repo.repo_stage_service import list_repo_ids_for_extraction


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="repo_extract_dag",
    default_args=default_args,
    description="Repository file extraction stage",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=max(1, settings.repo_pipeline_parallelism),
    tags=["repo", "extract", "parallel"],
) as dag:
    @task(task_id="extract_targets")
    def extract_targets() -> list[str]:
        return list_repo_ids_for_extraction()

    @task(
        task_id="extract_repo",
        max_active_tis_per_dag=max(1, settings.repo_pipeline_parallelism),
    )
    def extract_repo(repo_id: str) -> dict:
        return run_repo_file_extraction_for_repo(repo_id)

    target_repo_ids = extract_targets()
    extract_repo.expand(repo_id=target_repo_ids)
