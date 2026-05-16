from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from worker.common.config import settings
from worker.repo.pipeline.repo_retry_service import run_repo_processing_retry_for_shard


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="repo_retry_dag",
    default_args=default_args,
    description="Retry failed or delayed repository crawl/extract items",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=max(1, settings.repo_pipeline_parallelism),
    tags=["repo", "retry", "parallel"],
) as dag:
    shard_count = max(1, settings.repo_pipeline_parallelism)

    @task(
        task_id="retry_shard",
        max_active_tis_per_dag=max(1, settings.repo_pipeline_parallelism),
    )
    def retry_shard(shard_index: int) -> dict:
        return run_repo_processing_retry_for_shard(
            shard_index=shard_index,
            shard_count=shard_count,
        )

    retry_results = retry_shard.expand(shard_index=list(range(shard_count)))

    trigger_repo_chunk = TriggerDagRunOperator(
        task_id="trigger_repo_chunk_dag",
        trigger_dag_id="repo_chunk_dag",
    )

    retry_results >> trigger_repo_chunk
