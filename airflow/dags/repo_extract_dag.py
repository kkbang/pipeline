from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from worker.common.config import settings
from worker.repo.chunking.repo_chunk_phase import CHUNK_PHASE_LIGHT
from worker.repo.extraction.repo_file_extract_service import run_repo_file_extraction_for_shard
from worker.repo.pipeline.repo_chunk_planning_service import plan_chunk_shards_for_batch


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
    whale_shard_count = max(1, settings.repo_chunk_whale_phase_parallelism)

    @task(
        task_id="extract_shard",
        max_active_tis_per_dag=max(1, settings.repo_pipeline_parallelism),
    )
    def extract_shard(shard_index: int, dag_run: DagRun | None = None) -> dict:
        batch_id = dag_run.conf.get("batch_id") if dag_run and dag_run.conf else None
        return run_repo_file_extraction_for_shard(
            shard_index=shard_index,
            shard_count=shard_count,
            batch_id=batch_id,
        )

    extract_results = extract_shard.expand(shard_index=list(range(shard_count)))

    @task(task_id="plan_chunk_shards")
    def plan_chunk_shards(dag_run: DagRun | None = None) -> dict:
        batch_id = dag_run.conf.get("batch_id") if dag_run and dag_run.conf else None
        return plan_chunk_shards_for_batch(
            batch_id=batch_id,
            shard_count=shard_count,
            whale_shard_count=whale_shard_count,
        )

    chunk_plan = plan_chunk_shards()

    trigger_repo_chunk = TriggerDagRunOperator(
        task_id="trigger_repo_chunk_dag",
        trigger_dag_id="repo_chunk_dag",
        conf={
            "batch_id": "{{ dag_run.conf.get('batch_id') if dag_run and dag_run.conf else None }}",
            "phase": CHUNK_PHASE_LIGHT,
        },
    )

    extract_results >> chunk_plan >> trigger_repo_chunk
