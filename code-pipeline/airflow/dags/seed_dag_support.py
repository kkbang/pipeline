import json
import os
import re
from datetime import datetime
from pathlib import Path


SEED_CONFIG_PATH = Path("/opt/airflow/config/seed_packages.json")
CURATED_REPO_LIST_CONFIG_PATH = Path("/opt/airflow/config/curated_repo_lists.json")
BENCHMARK_DATASET_CONFIG_PATH = Path("/opt/airflow/config/benchmark_datasets.json")
GITHUB_ORG_LIST_CONFIG_PATH = Path("/opt/airflow/config/github_org_lists.json")
GITHUB_SEARCH_QUERY_CONFIG_PATH = Path("/opt/airflow/config/github_search_queries.json")
GITHUB_TOPIC_LIST_CONFIG_PATH = Path("/opt/airflow/config/github_topic_lists.json")
SEED_EXPANSION_RULE_CONFIG_PATH = Path("/opt/airflow/config/seed_expansion_rules.json")
GITHUB_SEARCH_POOL_NAME = (
    os.getenv("AIRFLOW_GITHUB_SEARCH_POOL", "github_search_pool").strip()
    or "github_search_pool"
)

SEED_DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}

SEED_DAG_START_DATE = datetime(2026, 1, 1)


def load_seed_packages(registry: str) -> list[str]:
    payload = json.loads(SEED_CONFIG_PATH.read_text(encoding="utf-8"))
    package_names = payload.get(registry, [])
    return [name for name in package_names if isinstance(name, str) and name.strip()]


def load_curated_repo_lists() -> dict[str, list[str | dict]]:
    payload = json.loads(CURATED_REPO_LIST_CONFIG_PATH.read_text(encoding="utf-8"))
    curated_lists = {}

    for list_name, repo_entries in payload.items():
        if not isinstance(list_name, str) or not isinstance(repo_entries, list):
            continue

        curated_lists[list_name] = [
            entry for entry in repo_entries if isinstance(entry, (str, dict))
        ]

    return curated_lists


def load_benchmark_datasets() -> dict[str, dict]:
    payload = json.loads(BENCHMARK_DATASET_CONFIG_PATH.read_text(encoding="utf-8"))
    benchmark_datasets = {}

    for dataset_name, dataset_config in payload.items():
        if not isinstance(dataset_name, str) or not isinstance(dataset_config, dict):
            continue

        if dataset_config.get("enabled") is not True:
            continue

        benchmark_datasets[dataset_name] = dataset_config

    return benchmark_datasets


def load_github_org_lists() -> dict[str, list[str]]:
    payload = json.loads(GITHUB_ORG_LIST_CONFIG_PATH.read_text(encoding="utf-8"))
    org_lists = {}

    for list_name, org_names in payload.items():
        if not isinstance(list_name, str) or not isinstance(org_names, list):
            continue

        cleaned_org_names = [
            org_name for org_name in org_names if isinstance(org_name, str) and org_name.strip()
        ]
        if cleaned_org_names:
            org_lists[list_name] = cleaned_org_names

    return org_lists


def load_github_search_queries() -> dict[str, list[dict]]:
    payload = json.loads(GITHUB_SEARCH_QUERY_CONFIG_PATH.read_text(encoding="utf-8"))
    search_lists = {}

    for list_name, search_queries in payload.items():
        if not isinstance(list_name, str) or not isinstance(search_queries, list):
            continue

        cleaned_queries = []
        for search_query in search_queries:
            if not isinstance(search_query, dict):
                continue
            query = search_query.get("query")
            if isinstance(query, str) and query.strip():
                cleaned_queries.append(search_query)

        if cleaned_queries:
            search_lists[list_name] = cleaned_queries

    return search_lists


def load_github_topic_lists() -> dict[str, list[dict]]:
    payload = json.loads(GITHUB_TOPIC_LIST_CONFIG_PATH.read_text(encoding="utf-8"))
    topic_lists = {}

    for list_name, topic_entries in payload.items():
        if not isinstance(list_name, str) or not isinstance(topic_entries, list):
            continue

        cleaned_entries = []
        for topic_entry in topic_entries:
            if not isinstance(topic_entry, dict):
                continue
            topic = topic_entry.get("topic")
            if isinstance(topic, str) and topic.strip():
                cleaned_entries.append(topic_entry)

        if cleaned_entries:
            topic_lists[list_name] = cleaned_entries

    return topic_lists


def load_seed_expansion_rules(rule_type: str) -> dict[str, dict]:
    payload = json.loads(SEED_EXPANSION_RULE_CONFIG_PATH.read_text(encoding="utf-8"))
    rules = {}

    for rule_name, rule_config in payload.items():
        if not isinstance(rule_name, str) or not isinstance(rule_config, dict):
            continue
        if rule_config.get("enabled") is not True:
            continue
        if rule_config.get("rule_type") != rule_type:
            continue
        rules[rule_name] = rule_config

    return rules


def slugify_task_suffix(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")
