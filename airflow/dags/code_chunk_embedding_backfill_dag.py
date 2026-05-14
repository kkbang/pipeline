from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from worker.repo.code_chunk_embedding_backfill_service import (
    has_pending_code_chunk_embedding_backfill_work,
    is_code_chunk_embedding_backfill_enabled,
    run_code_chunk_embedding_backfill,
)
from worker.common.config import settings


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="code_chunk_embedding_backfill_dag",
    default_args=default_args,
    description="Backfill embeddings for existing code chunk documents",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["repo", "chunk", "embedding", "backfill"],
) as dag:
    @task(task_id="backfill_code_chunk_embeddings")
    def backfill_embeddings(dag_run: DagRun | None = None) -> dict:
        if not is_code_chunk_embedding_backfill_enabled():
            raise AirflowSkipException(
                "Code chunk embedding backfill is disabled; enable CODE_CHUNK_EMBEDDING_ENABLED and configure endpoint/model."
            )
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        return run_code_chunk_embedding_backfill(
            repo_id=conf.get("repo_id"),
            limit=conf.get("limit"),
            scan_size=conf.get("scan_size"),
            write_batch_size=conf.get("write_batch_size"),
            write_buffer_size=conf.get("write_buffer_size"),
            force_reembed=conf.get("force_reembed", False),
            refresh_writes=conf.get("refresh_writes", False),
            skip_writes=conf.get("skip_writes", False),
        )

    backfill_result = backfill_embeddings()

    @task.short_circuit(task_id="has_pending_embedding_backfill_work")
    def has_pending_work(dag_run: DagRun | None = None) -> bool:
        if not is_code_chunk_embedding_backfill_enabled():
            return False
        if not settings.code_chunk_embedding_backfill_self_loop_enabled:
            return False
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        return has_pending_code_chunk_embedding_backfill_work(
            repo_id=conf.get("repo_id"),
            force_reembed=conf.get("force_reembed", False),
            skip_writes=conf.get("skip_writes", False),
        )

    pending_work = has_pending_work()

    trigger_next_backfill = TriggerDagRunOperator(
        task_id="trigger_next_code_chunk_embedding_backfill_dag",
        trigger_dag_id="code_chunk_embedding_backfill_dag",
        conf={
            "repo_id": "{{ dag_run.conf.get('repo_id') if dag_run and dag_run.conf else None }}",
            "limit": "{{ dag_run.conf.get('limit') if dag_run and dag_run.conf else None }}",
            "scan_size": "{{ dag_run.conf.get('scan_size') if dag_run and dag_run.conf else None }}",
            "write_batch_size": "{{ dag_run.conf.get('write_batch_size') if dag_run and dag_run.conf else None }}",
            "write_buffer_size": "{{ dag_run.conf.get('write_buffer_size') if dag_run and dag_run.conf else None }}",
            "force_reembed": "{{ dag_run.conf.get('force_reembed') if dag_run and dag_run.conf else False }}",
            "refresh_writes": "{{ dag_run.conf.get('refresh_writes') if dag_run and dag_run.conf else False }}",
            "skip_writes": "{{ dag_run.conf.get('skip_writes') if dag_run and dag_run.conf else False }}",
        },
    )

    backfill_result >> pending_work >> trigger_next_backfill
