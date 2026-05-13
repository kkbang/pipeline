import os
from dataclasses import dataclass


def _parse_github_tokens_from_env() -> tuple[str, ...]:
    raw_values = []
    primary = os.getenv("GITHUB_TOKEN", "")
    if isinstance(primary, str) and primary.strip():
        raw_values.append(primary.strip())

    token_bundle = os.getenv("GITHUB_TOKENS", "")
    if isinstance(token_bundle, str) and token_bundle.strip():
        normalized_bundle = token_bundle.replace("\r", "\n")
        for chunk in normalized_bundle.replace(",", "\n").split("\n"):
            token = chunk.strip()
            if token:
                raw_values.append(token)

    deduped = []
    seen = set()
    for token in raw_values:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)

    return tuple(deduped)


@dataclass
class Settings:
    app_env: str = os.getenv("APP_ENV", "local")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    opensearch_host: str = os.getenv("OPENSEARCH_HOST", "")
    opensearch_port: int = int(os.getenv("OPENSEARCH_PORT", "9200"))
    opensearch_user: str = os.getenv("OPENSEARCH_USER", "")
    opensearch_password: str = os.getenv("OPENSEARCH_PASSWORD", "")
    opensearch_use_ssl: bool = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"
    opensearch_bulk_flush_docs: int = int(os.getenv("OPENSEARCH_BULK_FLUSH_DOCS", "500"))

    github_token: str = os.getenv("GITHUB_TOKEN", "")
    github_tokens: tuple[str, ...] = _parse_github_tokens_from_env()
    request_timeout_seconds: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    github_org_repo_limit: int = int(os.getenv("GITHUB_ORG_REPO_LIMIT", "100"))
    github_search_repo_limit: int = int(os.getenv("GITHUB_SEARCH_REPO_LIMIT", "100"))
    github_topic_repo_limit: int = int(os.getenv("GITHUB_TOPIC_REPO_LIMIT", "100"))
    seed_github_global_concurrency: int = int(
        os.getenv(
            "SEED_GITHUB_GLOBAL_CONCURRENCY",
            os.getenv("SEED_GITHUB_INGESTION_CONCURRENCY", "4"),
        )
    )
    # Legacy alias. Keep reading for backward compatibility.
    seed_github_ingestion_concurrency: int = int(
        os.getenv(
            "SEED_GITHUB_INGESTION_CONCURRENCY",
            str(seed_github_global_concurrency),
        )
    )
    seed_github_search_concurrency: int = int(
        os.getenv(
            "SEED_GITHUB_SEARCH_CONCURRENCY",
            str(seed_github_global_concurrency),
        )
    )
    seed_github_core_concurrency: int = int(
        os.getenv(
            "SEED_GITHUB_CORE_CONCURRENCY",
            str(seed_github_global_concurrency),
        )
    )
    github_secondary_limit_base_backoff_sec: int = int(
        os.getenv("GITHUB_SECONDARY_LIMIT_BASE_BACKOFF_SEC", "4")
    )
    github_max_backoff_sec: int = int(os.getenv("GITHUB_MAX_BACKOFF_SEC", "120"))
    github_bucket_cooldown_jitter_sec: float = float(
        os.getenv("GITHUB_BUCKET_COOLDOWN_JITTER_SEC", "1.5")
    )
    github_tor_rotation_enabled: bool = (
        os.getenv("GITHUB_TOR_ROTATION_ENABLED", "false").lower() == "true"
    )
    github_tor_rotate_only_on_search: bool = (
        os.getenv("GITHUB_TOR_ROTATE_ONLY_ON_SEARCH", "true").lower() == "true"
    )
    github_tor_control_host: str = os.getenv("GITHUB_TOR_CONTROL_HOST", "tor-proxy")
    github_tor_control_port: int = int(os.getenv("GITHUB_TOR_CONTROL_PORT", "9051"))
    github_tor_control_password: str = os.getenv("GITHUB_TOR_CONTROL_PASSWORD", "")
    github_tor_rotation_min_interval_sec: int = int(
        os.getenv("GITHUB_TOR_ROTATION_MIN_INTERVAL_SEC", "45")
    )
    github_tor_newnym_wait_sec: float = float(
        os.getenv("GITHUB_TOR_NEWNYM_WAIT_SEC", "8")
    )
    seed_fail_soft_min_success_count: int = int(
        os.getenv("SEED_FAIL_SOFT_MIN_SUCCESS_COUNT", "1")
    )
    seed_fail_soft_min_success_ratio: float = float(
        os.getenv("SEED_FAIL_SOFT_MIN_SUCCESS_RATIO", "0.25")
    )
    seed_incremental_enabled: bool = (
        os.getenv("SEED_INCREMENTAL_ENABLED", "true").lower() == "true"
    )
    seed_incremental_min_interval_minutes: int = int(
        os.getenv("SEED_INCREMENTAL_MIN_INTERVAL_MINUTES", "180")
    )
    seed_incremental_overlap_days: int = int(
        os.getenv("SEED_INCREMENTAL_OVERLAP_DAYS", "2")
    )
    seed_normalize_parallelism: int = int(
        os.getenv(
            "SEED_NORMALIZE_PARALLELISM",
            os.getenv("SEED_NORMALIZATION_PARALLELISM", "8"),
        )
    )
    seed_qualify_parallelism: int = int(
        os.getenv("SEED_QUALIFY_PARALLELISM", "2")
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
    repo_crawl_concurrency: int = int(os.getenv("REPO_CRAWL_CONCURRENCY", "8"))
    repo_crawl_download_concurrency: int = int(
        os.getenv("REPO_CRAWL_DOWNLOAD_CONCURRENCY", str(repo_crawl_concurrency))
    )
    repo_crawl_extract_concurrency: int = int(
        os.getenv("REPO_CRAWL_EXTRACT_CONCURRENCY", str(max(1, repo_crawl_concurrency // 2)))
    )
    repo_crawl_lease_seconds: int = int(os.getenv("REPO_CRAWL_LEASE_SECONDS", "1800"))
    repo_crawl_repo_limit: int = int(os.getenv("REPO_CRAWL_REPO_LIMIT", "0"))
    repo_crawl_max_downloaded_backlog: int = int(
        os.getenv("REPO_CRAWL_MAX_DOWNLOADED_BACKLOG", "2000")
    )
    repo_crawl_max_extracted_backlog: int = int(
        os.getenv("REPO_CRAWL_MAX_EXTRACTED_BACKLOG", "5000")
    )
    repo_retry_repo_limit: int = int(os.getenv("REPO_RETRY_REPO_LIMIT", "500"))
    repo_retry_stale_after_seconds: int = int(
        os.getenv("REPO_RETRY_STALE_AFTER_SECONDS", str(repo_crawl_lease_seconds))
    )
    repo_whale_hint_size_kb: int = int(os.getenv("REPO_WHALE_HINT_SIZE_KB", "50000"))
    repo_giant_hint_size_kb: int = int(os.getenv("REPO_GIANT_HINT_SIZE_KB", "200000"))
    repo_file_extract_repo_limit: int = int(os.getenv("REPO_FILE_EXTRACT_REPO_LIMIT", "1000"))
    repo_file_extract_max_file_size_bytes: int = int(
        os.getenv("REPO_FILE_EXTRACT_MAX_FILE_SIZE_BYTES", "1048576")
    )
    repo_chunk_repo_limit: int = int(os.getenv("REPO_CHUNK_REPO_LIMIT", "1000"))
    repo_chunk_max_lines: int = int(os.getenv("REPO_CHUNK_MAX_LINES", "80"))
    repo_chunk_overlap_lines: int = int(os.getenv("REPO_CHUNK_OVERLAP_LINES", "20"))
    repo_chunk_whale_repo_min_code_bytes: int = int(
        os.getenv("REPO_CHUNK_WHALE_REPO_MIN_CODE_BYTES", "25000000")
    )
    repo_chunk_whale_repo_min_code_files: int = int(
        os.getenv("REPO_CHUNK_WHALE_REPO_MIN_CODE_FILES", "500")
    )
    repo_chunk_whale_phase_parallelism: int = int(
        os.getenv("REPO_CHUNK_WHALE_PHASE_PARALLELISM", "1")
    )
    repo_chunk_whale_file_parallelism: int = int(
        os.getenv("REPO_CHUNK_WHALE_FILE_PARALLELISM", "8")
    )
    code_chunk_index_alias: str = (
        os.getenv("CODE_CHUNK_INDEX_ALIAS", "code_chunk_index").strip() or "code_chunk_index"
    )
    code_chunk_index_version: str = os.getenv("CODE_CHUNK_INDEX_VERSION", "v1").strip() or "v1"
    code_chunk_embedding_index_alias: str = (
        os.getenv("CODE_CHUNK_EMBEDDING_INDEX_ALIAS", "code_chunk_embedding_index").strip()
        or "code_chunk_embedding_index"
    )
    code_chunk_embedding_index_version: str = (
        os.getenv("CODE_CHUNK_EMBEDDING_INDEX_VERSION", "v1").strip() or "v1"
    )
    code_chunk_chunker_version: str = (
        os.getenv("CODE_CHUNK_CHUNKER_VERSION", "ts_chunker_v1").strip() or "ts_chunker_v1"
    )
    code_chunk_normalization_version: str = (
        os.getenv("CODE_CHUNK_NORMALIZATION_VERSION", "norm_v1").strip() or "norm_v1"
    )
    code_chunk_anonymization_version: str = (
        os.getenv("CODE_CHUNK_ANONYMIZATION_VERSION", "anon_v2").strip() or "anon_v2"
    )
    code_chunk_feature_version: str = (
        os.getenv("CODE_CHUNK_FEATURE_VERSION", "ts_feature_v1").strip() or "ts_feature_v1"
    )
    code_chunk_identifier_token_cap: int = int(
        os.getenv("CODE_CHUNK_IDENTIFIER_TOKEN_CAP", "64")
    )
    code_chunk_call_token_cap: int = int(
        os.getenv("CODE_CHUNK_CALL_TOKEN_CAP", "64")
    )
    code_chunk_ast_node_sequence_cap: int = int(
        os.getenv("CODE_CHUNK_AST_NODE_SEQUENCE_CAP", "64")
    )
    code_chunk_min_symbol_lines: int = int(
        os.getenv("CODE_CHUNK_MIN_SYMBOL_LINES", "1")
    )
    code_chunk_max_symbol_lines: int = int(
        os.getenv("CODE_CHUNK_MAX_SYMBOL_LINES", "800")
    )
    code_chunk_raw_embedding_dimensions: int = int(
        os.getenv(
            "CODE_CHUNK_RAW_EMBEDDING_DIMENSIONS",
            os.getenv("OPENSEARCH_EMBEDDING_DIMENSIONS", "1536"),
        )
    )
    code_chunk_anonymized_embedding_dimensions: int = int(
        os.getenv(
            "CODE_CHUNK_ANONYMIZED_EMBEDDING_DIMENSIONS",
            os.getenv("OPENSEARCH_EMBEDDING_DIMENSIONS", "1536"),
        )
    )
    code_chunk_embedding_enabled: bool = (
        os.getenv(
            "CODE_CHUNK_EMBEDDING_ENABLED",
            os.getenv("OPENSEARCH_EMBEDDING_ENABLED", "false"),
        ).lower()
        == "true"
    )
    code_chunk_embedding_endpoint: str = (
        os.getenv(
            "CODE_CHUNK_EMBEDDING_ENDPOINT",
            os.getenv("OPENSEARCH_EMBEDDING_ENDPOINT", "https://api.openai.com/v1/embeddings"),
        ).strip()
    )
    code_chunk_embedding_api_key: str = (
        os.getenv(
            "CODE_CHUNK_EMBEDDING_API_KEY",
            os.getenv(
                "OPENSEARCH_EMBEDDING_API_KEY",
                os.getenv("OPENAI_API_KEY", ""),
            ),
        ).strip()
    )
    code_chunk_embedding_model: str = (
        os.getenv(
            "CODE_CHUNK_EMBEDDING_MODEL",
            os.getenv("OPENSEARCH_EMBEDDING_MODEL", ""),
        ).strip()
    )
    code_chunk_embedding_batch_size: int = int(
        os.getenv(
            "CODE_CHUNK_EMBEDDING_BATCH_SIZE",
            os.getenv("OPENSEARCH_EMBEDDING_BATCH_SIZE", "32"),
        )
    )
    code_chunk_embedding_parallelism: int = int(
        os.getenv(
            "CODE_CHUNK_EMBEDDING_PARALLELISM",
            os.getenv("OPENSEARCH_EMBEDDING_PARALLELISM", "4"),
        )
    )
    code_chunk_embedding_timeout_seconds: int = int(
        os.getenv(
            "CODE_CHUNK_EMBEDDING_TIMEOUT_SECONDS",
            os.getenv("OPENSEARCH_EMBEDDING_TIMEOUT_SECONDS", str(request_timeout_seconds)),
        )
    )
    code_chunk_embedding_fail_hard: bool = (
        os.getenv(
            "CODE_CHUNK_EMBEDDING_FAIL_HARD",
            os.getenv("OPENSEARCH_EMBEDDING_FAIL_HARD", "false"),
        ).lower()
        == "true"
    )
    repo_validation_repo_limit: int = int(os.getenv("REPO_VALIDATION_REPO_LIMIT", "1000"))
    repo_pipeline_batch_size: int = int(os.getenv("REPO_PIPELINE_BATCH_SIZE", "200"))
    repo_pipeline_parallelism: int = int(os.getenv("REPO_PIPELINE_PARALLELISM", "36"))
    repo_pipeline_self_loop_enabled: bool = (
        os.getenv("REPO_PIPELINE_SELF_LOOP_ENABLED", "true").lower() == "true"
    )
settings = Settings()
