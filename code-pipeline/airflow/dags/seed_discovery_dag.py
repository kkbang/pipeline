from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from seed_dag_support import (
    GITHUB_SEARCH_POOL_NAME,
    SEED_DAG_START_DATE,
    SEED_DEFAULT_ARGS,
    load_benchmark_datasets,
    load_curated_repo_lists,
    load_github_org_lists,
    load_github_search_queries,
    load_github_topic_lists,
    load_seed_expansion_rules,
    load_seed_packages,
    slugify_task_suffix,
)
from worker.seed.services.benchmark_artifact_fetch_service import run_benchmark_artifact_fetch
from worker.seed.services.seed_ingest_service import (
    run_benchmark_dataset_ingestion,
    run_curated_repo_ingestion,
    run_github_org_ingestion,
    run_github_search_ingestion,
    run_github_topic_ingestion,
    run_seed_ingestion,
)
from worker.seed.services.seed_normalize_service import run_seed_normalization
from worker.seed.services.seed_qualify_service import run_seed_qualification


with DAG(
    dag_id="seed_discovery_dag",
    default_args=SEED_DEFAULT_ARGS,
    description="Seed discovery pipeline",
    start_date=SEED_DAG_START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["seed", "discovery"],
) as dag:
    package_registries = [
        "pypi",
        "npm",
        "nuget",
        "maven",
        "cratesio",
        "hexpm",
        "rubygems",
        "cpan",
        "hackage",
        "cocoapods",
        "packagist",
        "pubdev",
        "gomod",
        "swiftpm",
    ]
    package_registry_ingest_tasks = []
    for registry in package_registries:
        package_registry_ingest_tasks.append(
            PythonOperator(
                task_id=f"seed_ingestion_{registry}",
                python_callable=run_seed_ingestion,
                op_kwargs={
                    "registry": registry,
                    "package_names": load_seed_packages(registry),
                },
            )
        )

    curated_ingest_tasks = []
    for list_name, repo_entries in load_curated_repo_lists().items():
        curated_ingest_tasks.append(
            PythonOperator(
                task_id=f"seed_ingestion_curated_{slugify_task_suffix(list_name)}",
                python_callable=run_curated_repo_ingestion,
                op_kwargs={
                    "list_name": list_name,
                    "repo_entries": repo_entries,
                },
            )
        )

    github_org_ingest_tasks = []
    for list_name, org_names in load_github_org_lists().items():
        github_org_ingest_tasks.append(
            PythonOperator(
                task_id=f"seed_ingestion_github_org_{slugify_task_suffix(list_name)}",
                python_callable=run_github_org_ingestion,
                op_kwargs={
                    "list_name": list_name,
                    "org_names": org_names,
                },
            )
        )

    github_search_ingest_tasks = []
    for list_name, search_queries in load_github_search_queries().items():
        github_search_ingest_tasks.append(
            PythonOperator(
                task_id=f"seed_ingestion_github_search_{slugify_task_suffix(list_name)}",
                python_callable=run_github_search_ingestion,
                pool=GITHUB_SEARCH_POOL_NAME,
                op_kwargs={
                    "list_name": list_name,
                    "search_queries": search_queries,
                },
            )
        )

    github_topic_ingest_tasks = []
    for list_name, topic_entries in load_github_topic_lists().items():
        github_topic_ingest_tasks.append(
            PythonOperator(
                task_id=f"seed_ingestion_github_topic_{slugify_task_suffix(list_name)}",
                python_callable=run_github_topic_ingestion,
                pool=GITHUB_SEARCH_POOL_NAME,
                op_kwargs={
                    "list_name": list_name,
                    "topic_entries": topic_entries,
                },
            )
        )

    benchmark_ingest_tasks = []
    for dataset_name, dataset_config in load_benchmark_datasets().items():
        ingest_task = PythonOperator(
            task_id=f"seed_ingestion_benchmark_{slugify_task_suffix(dataset_name)}",
            python_callable=run_benchmark_dataset_ingestion,
            op_kwargs={
                "dataset_name": dataset_name,
                "dataset_config": dataset_config,
            },
        )
        benchmark_ingest_tasks.append(ingest_task)

        artifact_fetch = dataset_config.get("artifact_fetch") or {}
        if artifact_fetch.get("enabled") is True:
            fetch_task = PythonOperator(
                task_id=f"benchmark_artifact_fetch_{slugify_task_suffix(dataset_name)}",
                python_callable=run_benchmark_artifact_fetch,
                op_kwargs={
                    "dataset_name": dataset_name,
                    "dataset_config": dataset_config,
                },
            )
            fetch_task >> ingest_task

    normalize_task = PythonOperator(
        task_id="seed_normalization",
        python_callable=run_seed_normalization,
    )

    qualify_task = PythonOperator(
        task_id="seed_qualification",
        python_callable=run_seed_qualification,
    )

    base_ingest_tasks = [
        *package_registry_ingest_tasks,
        *curated_ingest_tasks,
        *github_org_ingest_tasks,
        *github_search_ingest_tasks,
        *github_topic_ingest_tasks,
        *benchmark_ingest_tasks,
    ]
    base_ingest_tasks >> normalize_task >> qualify_task

    content_rules = load_seed_expansion_rules("repo_content_expansion")
    relation_rules = load_seed_expansion_rules("repo_relation_enrichment")

    if content_rules:
        qualify_task >> TriggerDagRunOperator(
            task_id="trigger_seed_expansion_dag",
            trigger_dag_id="seed_expansion_dag",
            wait_for_completion=False,
        )
    elif relation_rules:
        qualify_task >> TriggerDagRunOperator(
            task_id="trigger_repo_relation_dag",
            trigger_dag_id="repo_relation_dag",
            wait_for_completion=False,
        )
