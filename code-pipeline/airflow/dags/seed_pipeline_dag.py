from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from seed_dag_support import SEED_DAG_START_DATE, SEED_DEFAULT_ARGS


with DAG(
    dag_id="seed_pipeline_dag",
    default_args=SEED_DEFAULT_ARGS,
    description="Compatibility DAG that triggers the seed discovery pipeline",
    start_date=SEED_DAG_START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["seed", "compatibility"],
) as dag:
    TriggerDagRunOperator(
        task_id="trigger_seed_discovery_dag",
        trigger_dag_id="seed_discovery_dag",
        wait_for_completion=False,
    )
