from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from worker.common.config import settings
from worker.repo.chunking.repo_chunk_phase import (
    CHUNK_PHASE_LIGHT,
    CHUNK_PHASE_WHALE,
    normalize_chunk_phase,
)
from worker.repo.chunking.repo_chunk_service import run_repo_code_chunking_for_shard
from worker.repo.pipeline.repo_stage_service import has_pending_chunk_work


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
    light_shard_count = max(1, settings.repo_pipeline_parallelism)
    whale_shard_count = max(1, settings.repo_chunk_whale_phase_parallelism)

    def _phase_shard_count(phase: str) -> int:
        if phase == CHUNK_PHASE_WHALE:
            return whale_shard_count
        return light_shard_count

    @task(task_id="resolve_shard_indices")
    def resolve_shard_indices(dag_run: DagRun | None = None) -> list[int]:
        phase = normalize_chunk_phase(dag_run.conf.get("phase") if dag_run and dag_run.conf else None)
        return list(range(_phase_shard_count(phase)))

    @task(
        task_id="chunk_shard",
        max_active_tis_per_dag=max(1, settings.repo_pipeline_parallelism),
    )
    def chunk_shard(shard_index: int, dag_run: DagRun | None = None) -> dict:
        batch_id = dag_run.conf.get("batch_id") if dag_run and dag_run.conf else None
        phase = normalize_chunk_phase(dag_run.conf.get("phase") if dag_run and dag_run.conf else None)
        return run_repo_code_chunking_for_shard(
            shard_index=shard_index,
            shard_count=_phase_shard_count(phase),
            batch_id=batch_id,
            phase=phase,
        )

    chunk_results = chunk_shard.expand(shard_index=resolve_shard_indices())

    trigger_whale_chunk = TriggerDagRunOperator(
        task_id="trigger_whale_repo_chunk_dag",
        trigger_dag_id="repo_chunk_dag",
        conf={
            "batch_id": "{{ dag_run.conf.get('batch_id') if dag_run and dag_run.conf else None }}",
            "phase": CHUNK_PHASE_WHALE,
        },
    )

    @task.branch(task_id="route_next_phase")
    def route_next_phase(dag_run: DagRun | None = None) -> str:
        batch_id = dag_run.conf.get("batch_id") if dag_run and dag_run.conf else None
        phase = normalize_chunk_phase(dag_run.conf.get("phase") if dag_run and dag_run.conf else None)
        if phase == CHUNK_PHASE_WHALE:
            return "trigger_repo_validation_dag"
        if has_pending_chunk_work(
            batch_id=batch_id,
            phase=CHUNK_PHASE_WHALE,
            require_light_phase_cleared=True,
        ):
            return "trigger_whale_repo_chunk_dag"
        return "trigger_repo_validation_dag"

    trigger_repo_validation = TriggerDagRunOperator(
        task_id="trigger_repo_validation_dag",
        trigger_dag_id="repo_validation_dag",
        conf={"batch_id": "{{ dag_run.conf.get('batch_id') if dag_run and dag_run.conf else None }}"},
    )

    next_phase = route_next_phase()

    chunk_results >> next_phase
    next_phase >> trigger_whale_chunk
    next_phase >> trigger_repo_validation
