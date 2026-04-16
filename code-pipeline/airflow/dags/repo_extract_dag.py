from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from worker.common.config import settings
from worker.repo.repo_file_extract_service import run_repo_file_extraction_for_shard


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
    shard_count = max(1, settings.repo_pipeline_parallelism)

    @task(
        task_id="extract_shard",
        max_active_tis_per_dag=max(1, settings.repo_pipeline_parallelism),
    )
    def extract_shard(shard_index: int) -> dict:
        return run_repo_file_extraction_for_shard(
            shard_index=shard_index,
            shard_count=shard_count,
        )

    extract_shard.expand(shard_index=list(range(shard_count)))
