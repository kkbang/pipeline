from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from worker.common.config import settings
from worker.repo.repo_crawl_service import (
    get_pending_repo_crawl_stats,
    should_pause_repo_crawl_due_to_backlog,
)
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

    validation_results = validate_shard.expand(shard_index=list(range(shard_count)))

    @task.short_circuit(task_id="has_pending_repo_crawl_work")
    def has_pending_repo_crawl_work() -> bool:
        if not settings.repo_pipeline_self_loop_enabled:
            return False
        crawl_paused, _pause_reasons = should_pause_repo_crawl_due_to_backlog()
        if crawl_paused:
            return False
        pending_stats = get_pending_repo_crawl_stats()
        return int(pending_stats.get("pending_count") or 0) > 0

    pending_crawl_work = has_pending_repo_crawl_work()

    trigger_next_code_pipeline = TriggerDagRunOperator(
        task_id="trigger_next_code_pipeline_dag",
        trigger_dag_id="code_pipeline_dag",
    )

    validation_results >> pending_crawl_work >> trigger_next_code_pipeline
