import os
from dataclasses import dataclass


@dataclass
class Settings:
    app_env: str = os.getenv("APP_ENV", "local")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    aws_region: str = os.getenv("AWS_REGION", "ap-northeast-2")
    s3_bucket: str = os.getenv("S3_BUCKET", "")

    opensearch_host: str = os.getenv("OPENSEARCH_HOST", "")
    opensearch_port: int = int(os.getenv("OPENSEARCH_PORT", "9200"))
    opensearch_user: str = os.getenv("OPENSEARCH_USER", "")
    opensearch_password: str = os.getenv("OPENSEARCH_PASSWORD", "")
    opensearch_use_ssl: bool = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"

    github_token: str = os.getenv("GITHUB_TOKEN", "")
    request_timeout_seconds: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    github_org_repo_limit: int = int(os.getenv("GITHUB_ORG_REPO_LIMIT", "100"))
    github_search_repo_limit: int = int(os.getenv("GITHUB_SEARCH_REPO_LIMIT", "100"))
    github_topic_repo_limit: int = int(os.getenv("GITHUB_TOPIC_REPO_LIMIT", "100"))
    seed_github_ingestion_concurrency: int = int(
        os.getenv("SEED_GITHUB_INGESTION_CONCURRENCY", "4")
    )
    benchmark_data_dir: str = os.getenv("BENCHMARK_DATA_DIR", "/opt/airflow/benchmark_data")
    keep_full_package_registry_raw: bool = (
        os.getenv("KEEP_FULL_PACKAGE_REGISTRY_RAW", "false").lower() == "true"
    )
    github_repo_metadata_concurrency: int = int(
        os.getenv(
            "GITHUB_REPO_METADATA_CONCURRENCY",
            os.getenv("GITHUB_REPO_RESOLVER_CONCURRENCY", "3"),
        )
    )
    repo_content_expansion_repo_limit: int = int(
        os.getenv("REPO_CONTENT_EXPANSION_REPO_LIMIT", "100")
    )
    repo_content_expansion_concurrency: int = int(
        os.getenv("REPO_CONTENT_EXPANSION_CONCURRENCY", "4")
    )
    repo_relation_repo_limit: int = int(os.getenv("REPO_RELATION_REPO_LIMIT", "200"))
    repo_crawl_concurrency: int = int(os.getenv("REPO_CRAWL_CONCURRENCY", "4"))
    repo_crawl_lease_seconds: int = int(os.getenv("REPO_CRAWL_LEASE_SECONDS", "1800"))
settings = Settings()
