from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from worker.repo.repo_crawl_service import repo_crawler


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="code_pipeline_dag",
    default_args=default_args,
    description="Repository snapshot download stage",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["repo", "snapshot", "download"],
) as dag:
    repo_snapshot_download_task = PythonOperator(
        task_id="repo_snapshot_download",
        python_callable=repo_crawler,
        op_kwargs={"batch_id": "{{ ts }}"},
    )

    trigger_repo_extract = TriggerDagRunOperator(
        task_id="trigger_repo_extract_dag",
        trigger_dag_id="repo_extract_dag",
        conf={"batch_id": "{{ ts }}"},
    )

    repo_snapshot_download_task >> trigger_repo_extract
