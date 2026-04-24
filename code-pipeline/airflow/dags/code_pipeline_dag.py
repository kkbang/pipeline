from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from worker.repo.repo_crawl_service import repo_crawler


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="code_pipeline_dag",
    default_args=default_args,
    description="Repository download-to-processing pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["repo", "snapshot"],
) as dag:
    repo_snapshot_download_task = PythonOperator(
        task_id="repo_snapshot_download",
        python_callable=repo_crawler,
    )
