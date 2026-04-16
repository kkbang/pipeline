from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from worker.common.config import settings
from worker.repo.repo_stage_service import list_repo_ids_for_validation
from worker.repo.repo_validation_service import run_repo_processing_validation_for_repo


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="repo_validation_dag",
    default_args=default_args,
    description="Repository processing validation stage",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=max(1, settings.repo_pipeline_parallelism),
    tags=["repo", "validation", "parallel"],
) as dag:
    @task(task_id="validation_targets")
    def validation_targets() -> list[str]:
        return list_repo_ids_for_validation()

    @task(
        task_id="validate_repo",
        max_active_tis_per_dag=max(1, settings.repo_pipeline_parallelism),
    )
    def validate_repo(repo_id: str) -> dict:
        return run_repo_processing_validation_for_repo(repo_id)

    target_repo_ids = validation_targets()
    validate_repo.expand(repo_id=target_repo_ids)
