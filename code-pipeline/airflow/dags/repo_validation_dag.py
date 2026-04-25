from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.dagrun import DagRun

from worker.common.config import settings
from worker.repo.repo_validation_service import run_repo_processing_validation_for_shard


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
    shard_count = max(1, settings.repo_pipeline_parallelism)

    @task(
        task_id="validate_shard",
        max_active_tis_per_dag=max(1, settings.repo_pipeline_parallelism),
    )
    def validate_shard(shard_index: int, dag_run: DagRun | None = None) -> dict:
        batch_id = dag_run.conf.get("batch_id") if dag_run and dag_run.conf else None
        return run_repo_processing_validation_for_shard(
            shard_index=shard_index,
            shard_count=shard_count,
            batch_id=batch_id,
        )

    validate_shard.expand(shard_index=list(range(shard_count)))
