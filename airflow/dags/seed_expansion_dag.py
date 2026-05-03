from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from seed_dag_support import (
    SEED_DAG_START_DATE,
    SEED_DEFAULT_ARGS,
    load_seed_expansion_rules,
    slugify_task_suffix,
)
from worker.seed.services.seed_expansion_service import run_repo_content_expansion
from worker.seed.services.seed_normalize_service import run_seed_normalization
from worker.seed.services.seed_qualify_service import run_seed_qualification


with DAG(
    dag_id="seed_expansion_dag",
    default_args=SEED_DEFAULT_ARGS,
    description="Seed expansion pipeline",
    start_date=SEED_DAG_START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["seed", "expansion"],
) as dag:
    content_rules = load_seed_expansion_rules("repo_content_expansion")
    relation_rules = load_seed_expansion_rules("repo_relation_enrichment")

    if content_rules:
        content_expansion_tasks = []
        for rule_name, rule_config in content_rules.items():
            content_expansion_tasks.append(
                PythonOperator(
                    task_id=f"repo_content_expansion_{slugify_task_suffix(rule_name)}",
                    python_callable=run_repo_content_expansion,
                    op_kwargs={
                        "rule_name": rule_name,
                        "rule_config": rule_config,
                    },
                )
            )

        expanded_normalize_task = PythonOperator(
            task_id="seed_normalization_expanded",
            python_callable=run_seed_normalization,
        )

        expanded_qualify_task = PythonOperator(
            task_id="seed_qualification_expanded",
            python_callable=run_seed_qualification,
        )

        content_expansion_tasks >> expanded_normalize_task >> expanded_qualify_task
        relation_upstream = expanded_qualify_task
    else:
        relation_upstream = EmptyOperator(task_id="no_repo_content_expansion_rules")

    if relation_rules:
        relation_upstream >> TriggerDagRunOperator(
            task_id="trigger_repo_relation_dag",
            trigger_dag_id="repo_relation_dag",
            wait_for_completion=False,
        )
    else:
        EmptyOperator(task_id="no_repo_relation_rules")
