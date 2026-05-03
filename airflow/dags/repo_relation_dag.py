from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from seed_dag_support import (
    SEED_DAG_START_DATE,
    SEED_DEFAULT_ARGS,
    load_seed_expansion_rules,
    slugify_task_suffix,
)
from worker.seed.services.repo_relation_service import run_repo_relation_enrichment


with DAG(
    dag_id="repo_relation_dag",
    default_args=SEED_DEFAULT_ARGS,
    description="Repository relation enrichment pipeline",
    start_date=SEED_DAG_START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["seed", "relation"],
) as dag:
    relation_rules = load_seed_expansion_rules("repo_relation_enrichment")

    if relation_rules:
        for rule_name, rule_config in relation_rules.items():
            PythonOperator(
                task_id=f"repo_relation_enrichment_{slugify_task_suffix(rule_name)}",
                python_callable=run_repo_relation_enrichment,
                op_kwargs={
                    "rule_name": rule_name,
                    "rule_config": rule_config,
                },
            )
    else:
        EmptyOperator(task_id="no_repo_relation_rules")
