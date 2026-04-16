from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from worker.common.config import settings
from worker.repo.repo_chunk_service import run_repo_code_chunking_for_repo
from worker.repo.repo_stage_service import list_repo_ids_for_chunking


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="repo_chunk_dag",
    default_args=default_args,
    description="Repository code chunking stage",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=max(1, settings.repo_pipeline_parallelism),
    tags=["repo", "chunk", "parallel"],
) as dag:
    @task(task_id="chunk_targets")
    def chunk_targets() -> list[str]:
        return list_repo_ids_for_chunking()

    @task(
        task_id="chunk_repo",
        max_active_tis_per_dag=max(1, settings.repo_pipeline_parallelism),
    )
    def chunk_repo(repo_id: str) -> dict:
        return run_repo_code_chunking_for_repo(repo_id)

    target_repo_ids = chunk_targets()
    chunk_repo.expand(repo_id=target_repo_ids)
