import asyncio
import logging
import re
from dataclasses import dataclass

from worker.common.config import settings
from worker.seed.adapters.benchmark import BenchmarkDatasetAdapter
from worker.seed.adapters.curated import CuratedRepoListAdapter
from worker.seed.adapters.github import (
    GitHubApiAdapter,
    GitHubOrgAdapter,
    GitHubSearchAdapter,
    GitHubTopicAdapter,
)
from worker.seed.adapters.package_registry import (
    CocoaPodsAdapter,
    CPANAdapter,
    CratesIOAdapter,
    GoModuleAdapter,
    HackageAdapter,
    HexPMAdapter,
    MavenCentralAdapter,
    NPMAdapter,
    NuGetAdapter,
    PackagistAdapter,
    PubDevAdapter,
    PyPIAdapter,
    RubyGemsAdapter,
    SwiftPMAdapter,
)
from worker.seed.services.seed_item_writer import (
    safe_path_fragment,
    stable_digest,
    write_seed_item,
)
from worker.seed.services.seed_incremental_cursor_service import (
    CursorUpdatePayload,
    build_incremental_pushed_filter_date,
    extract_max_timestamp,
    load_cursor,
    should_skip_by_min_interval,
    upsert_cursor,
)
from worker.storage.opensearch_store import OpenSearchStore

logger = logging.getLogger(__name__)
TIME_QUALIFIER_PATTERN = re.compile(r"\b(?:pushed|updated|created)\s*:", re.IGNORECASE)


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _has_time_qualifier(query: str) -> bool:
    return bool(TIME_QUALIFIER_PATTERN.search(query))


def _append_incremental_pushed_filter(query: str, pushed_since_date: str | None) -> str:
    normalized_query = _normalize_whitespace(query)
    if not pushed_since_date:
        return normalized_query
    if _has_time_qualifier(normalized_query):
        return normalized_query
    return f"{normalized_query} pushed:>={pushed_since_date}"


def _build_search_request_key(search_query: dict) -> tuple[str, str]:
    base_query = _normalize_whitespace(str(search_query.get("query") or ""))
    sort = _normalize_whitespace(str(search_query.get("sort") or "stars")) or "stars"
    order = _normalize_whitespace(str(search_query.get("order") or "desc")) or "desc"
    stable_key = stable_digest(f"search:{base_query}|sort:{sort}|order:{order}")
    return stable_key, base_query


def _build_topic_request_key(topic_entry: dict) -> tuple[str, str]:
    topic = _normalize_whitespace(str(topic_entry.get("topic") or ""))
    extra_query = _normalize_whitespace(str(topic_entry.get("query") or ""))
    sort = _normalize_whitespace(str(topic_entry.get("sort") or "stars")) or "stars"
    order = _normalize_whitespace(str(topic_entry.get("order") or "desc")) or "desc"
    stable_key = stable_digest(
        f"topic:{topic}|query:{extra_query}|sort:{sort}|order:{order}"
    )
    return stable_key, topic


@dataclass(slots=True)
class GitHubSeedFetchResult:
    key: str
    label: str
    request_query: str
    metadatas: list
    error: Exception | None = None


def _get_package_registry_adapter(
    registry: str,
) -> object:
    if registry == "pypi":
        return PyPIAdapter()

    if registry == "npm":
        return NPMAdapter()

    if registry == "nuget":
        return NuGetAdapter()

    if registry == "maven":
        return MavenCentralAdapter()

    if registry == "cratesio":
        return CratesIOAdapter()

    if registry == "hexpm":
        return HexPMAdapter()

    if registry == "rubygems":
        return RubyGemsAdapter()

    if registry == "cpan":
        return CPANAdapter()

    if registry == "hackage":
        return HackageAdapter()

    if registry == "cocoapods":
        return CocoaPodsAdapter()

    if registry == "packagist":
        return PackagistAdapter()

    if registry == "pubdev":
        return PubDevAdapter()

    if registry == "gomod":
        return GoModuleAdapter()

    if registry == "swiftpm":
        return SwiftPMAdapter()

    raise ValueError(f"Unsupported package registry: {registry}")


async def _fetch_github_seed_batch(
    items: list[tuple[str, str, str, object]],
    fetcher,
) -> list[GitHubSeedFetchResult]:
    semaphore = asyncio.Semaphore(max(1, settings.seed_github_global_concurrency))

    async def _fetch(
        key: str,
        label: str,
        request_query: str,
        payload: object,
    ) -> GitHubSeedFetchResult:
        async with semaphore:
            try:
                metadatas = await fetcher(payload)
                return GitHubSeedFetchResult(
                    key=key,
                    label=label,
                    request_query=request_query,
                    metadatas=metadatas,
                )
            except Exception as exc:
                return GitHubSeedFetchResult(
                    key=key,
                    label=label,
                    request_query=request_query,
                    metadatas=[],
                    error=exc,
                )

    return await asyncio.gather(
        *(
            _fetch(key, label, request_query, payload)
            for key, label, request_query, payload in items
        )
    )


def _raise_first_fetch_error(results: list[GitHubSeedFetchResult]) -> None:
    for result in results:
        if result.error is not None:
            raise result.error


def _finalize_fetch_results(
    results: list[GitHubSeedFetchResult],
    *,
    batch_type: str,
    source_name: str,
) -> None:
    total_requests = len(results)
    failed_requests = sum(1 for result in results if result.error is not None)
    successful_requests = total_requests - failed_requests
    emitted_items = sum(len(result.metadatas) for result in results)
    success_ratio = (successful_requests / total_requests) if total_requests > 0 else 1.0

    logger.info(
        (
            "GitHub ingestion batch summary: type=%s source=%s total_requests=%s "
            "successful_requests=%s failed_requests=%s success_ratio=%.3f emitted_items=%s"
        ),
        batch_type,
        source_name,
        total_requests,
        successful_requests,
        failed_requests,
        success_ratio,
        emitted_items,
    )

    if failed_requests == 0:
        return

    # Fail-soft policy:
    # - hard fail only when every fetch request failed.
    # - otherwise continue but emit degraded warnings if quality thresholds are not met.
    if successful_requests == 0:
        _raise_first_fetch_error(results)
        return

    if (
        successful_requests < settings.seed_fail_soft_min_success_count
        or success_ratio < settings.seed_fail_soft_min_success_ratio
    ):
        logger.warning(
            (
                "GitHub ingestion batch degraded: type=%s source=%s "
                "successful_requests=%s/%s success_ratio=%.3f "
                "min_success_count=%s min_success_ratio=%.3f"
            ),
            batch_type,
            source_name,
            successful_requests,
            total_requests,
            success_ratio,
            settings.seed_fail_soft_min_success_count,
            settings.seed_fail_soft_min_success_ratio,
        )


def run_seed_ingestion(registry: str = "pypi", package_names: list[str] | None = None) -> None:
    adapter = _get_package_registry_adapter(registry)
    store = OpenSearchStore()

    if not package_names:
        if registry == "pypi":
            package_names = ["requests", "flask", "django", "fastapi", "numpy", "pandas"]
        elif registry == "npm":
            package_names = ["react", "express", "lodash", "axios", "typescript", "vite"]
        elif registry == "nuget":
            package_names = [
                "Newtonsoft.Json",
                "Serilog",
                "Dapper",
                "MediatR",
                "AutoMapper",
                "FluentValidation",
            ]
        elif registry == "maven":
            package_names = [
                "org.springframework:spring-core",
                "com.fasterxml.jackson.core:jackson-databind",
                "org.apache.commons:commons-lang3",
                "org.slf4j:slf4j-api",
                "junit:junit",
                "org.mockito:mockito-core",
            ]
        elif registry == "cratesio":
            package_names = ["serde", "tokio", "reqwest", "axum", "clap", "sqlx"]
        elif registry == "hexpm":
            package_names = ["phoenix", "ecto", "plug", "jason", "nimble_parsec", "oban"]
        elif registry == "rubygems":
            package_names = ["rails", "sinatra", "sidekiq", "rspec", "rubocop", "nokogiri"]
        elif registry == "cpan":
            package_names = [
                "DBI",
                "Mojolicious",
                "Dancer2",
                "Plack",
                "Try-Tiny",
                "Moo",
            ]
        elif registry == "hackage":
            package_names = ["aeson", "text", "bytestring", "lens", "servant", "yesod"]
        elif registry == "cocoapods":
            package_names = [
                "AFNetworking",
                "Alamofire",
                "SnapKit",
                "RxSwift",
                "Kingfisher",
                "Moya",
            ]
        elif registry == "packagist":
            package_names = [
                "laravel/framework",
                "symfony/symfony",
                "guzzlehttp/guzzle",
                "monolog/monolog",
                "doctrine/orm",
                "phpunit/phpunit",
            ]
        elif registry == "pubdev":
            package_names = ["flutter", "dio", "riverpod", "provider", "bloc", "go_router"]
        elif registry == "gomod":
            package_names = [
                "github.com/gin-gonic/gin",
                "github.com/spf13/cobra",
                "github.com/gofiber/fiber/v2",
                "github.com/labstack/echo/v4",
                "github.com/stretchr/testify",
                "go.uber.org/zap",
            ]
        elif registry == "swiftpm":
            package_names = [
                "apple/swift-nio",
                "apple/swift-argument-parser",
                "pointfreeco/swift-composable-architecture",
                "realm/realm-swift",
                "Alamofire/Alamofire",
                "onevcat/Kingfisher",
            ]
        else:
            package_names = []

    success_count = 0
    first_error: Exception | None = None
    for package_name in package_names:
        normalized_package_name = str(package_name or "").strip()
        if not normalized_package_name:
            continue
        source_key = stable_digest(f"package:{registry}:{normalized_package_name}")
        if settings.seed_incremental_enabled:
            cursor = load_cursor(
                store,
                source_type="package_registry",
                source_name=registry,
                source_key=source_key,
            )
            if should_skip_by_min_interval(
                cursor,
                min_interval_minutes=settings.seed_incremental_min_interval_minutes,
            ):
                logger.info(
                    "Skip package registry by incremental interval: registry=%s package=%s",
                    registry,
                    normalized_package_name,
                )
                continue

        try:
            metadata = adapter.fetch_package_metadata(normalized_package_name)
        except Exception as exc:
            if first_error is None:
                first_error = exc
            logger.warning(
                "Package registry seed ingestion failed for registry=%s package=%s error=%s",
                registry,
                normalized_package_name,
                str(exc),
            )
            upsert_cursor(
                store,
                CursorUpdatePayload(
                    source_type="package_registry",
                    source_name=registry,
                    source_key=source_key,
                    request_label=normalized_package_name,
                    request_query=normalized_package_name,
                    emitted_count=0,
                    status="error",
                    error_message=str(exc),
                ),
            )
            continue

        doc_id = f"{registry}:{normalized_package_name}"
        if settings.keep_full_package_registry_raw and metadata.full_raw_metadata is not None:
            store.upsert_document(
                collection_name="package_registry_full_metadata_index",
                doc_id=doc_id,
                body={
                    "registry_name": registry,
                    "package_name": normalized_package_name,
                    "full_raw_metadata": metadata.full_raw_metadata,
                },
            )

        write_seed_item(
            store,
            doc_id=doc_id,
            source_type="package_registry_repo",
            source_name=registry,
            source_item_id=doc_id,
            raw_metadata=metadata.raw_metadata,
            candidate_repo_urls=metadata.candidate_repo_urls,
        )
        upsert_cursor(
            store,
            CursorUpdatePayload(
                source_type="package_registry",
                source_name=registry,
                source_key=source_key,
                request_label=normalized_package_name,
                request_query=normalized_package_name,
                emitted_count=1,
                status="success",
            ),
        )
        success_count += 1

    if success_count == 0 and first_error is not None:
        raise first_error


def run_curated_repo_ingestion(list_name: str, repo_entries: list[str | dict] | None = None) -> None:
    adapter = CuratedRepoListAdapter()
    store = OpenSearchStore()

    if not repo_entries:
        return

    for repo_entry in repo_entries:
        metadata = adapter.build_repo_metadata(list_name, repo_entry)

        doc_id = f"curated_repo_list:{list_name}:{metadata.source_item_id}"
        write_seed_item(
            store,
            doc_id=doc_id,
            source_type="curated_repo_list",
            source_name=list_name,
            source_item_id=metadata.source_item_id,
            raw_metadata=metadata.raw_metadata,
            candidate_repo_urls=metadata.candidate_repo_urls,
        )


def run_benchmark_dataset_ingestion(dataset_name: str, dataset_config: dict | None = None) -> None:
    adapter = BenchmarkDatasetAdapter()
    store = OpenSearchStore()

    if not dataset_config:
        return

    for metadata in adapter.build_repo_metadatas(dataset_name, dataset_config):
        doc_id = f"benchmark_dataset_repo:{dataset_name}:{metadata.source_item_id}"
        write_seed_item(
            store,
            doc_id=doc_id,
            source_type="benchmark_dataset_repo",
            source_name=dataset_name,
            source_item_id=metadata.source_item_id,
            raw_metadata=metadata.raw_metadata,
            candidate_repo_urls=metadata.candidate_repo_urls,
        )


def run_github_org_ingestion(list_name: str, org_names: list[str] | None = None) -> None:
    store = OpenSearchStore()

    if not org_names:
        return

    keyed_orgs: list[tuple[str, str, str, str]] = []
    for org_name in org_names:
        normalized_org = _normalize_whitespace(str(org_name))
        if not normalized_org:
            continue
        source_key = stable_digest(f"org:{normalized_org}")
        if settings.seed_incremental_enabled:
            cursor = load_cursor(
                store,
                source_type="github_org",
                source_name=list_name,
                source_key=source_key,
            )
            if should_skip_by_min_interval(
                cursor,
                min_interval_minutes=settings.seed_incremental_min_interval_minutes,
            ):
                logger.info(
                    "Skip github org by incremental interval: list=%s org=%s",
                    list_name,
                    normalized_org,
                )
                continue
        keyed_orgs.append(
            (
                source_key,
                normalized_org,
                f"org:{normalized_org}",
                normalized_org,
            )
        )

    if not keyed_orgs:
        logger.info("No github org entries to request after incremental filtering: list=%s", list_name)
        return

    async def _run() -> list[GitHubSeedFetchResult]:
        async with GitHubApiAdapter(concurrency=settings.seed_github_global_concurrency) as api_adapter:
            adapter = GitHubOrgAdapter(api_adapter)
            return await _fetch_github_seed_batch(
                keyed_orgs,
                lambda org_name: adapter.fetch_org_repositories(
                    list_name=list_name,
                    org_name=org_name,
                ),
            )

    fetch_results = asyncio.run(_run())

    for result in fetch_results:
        max_pushed_at = extract_max_timestamp(
            [
                (metadata.raw_metadata or {}).get("pushed_at")
                for metadata in result.metadatas
            ]
        )
        max_updated_at = extract_max_timestamp(
            [
                (metadata.raw_metadata or {}).get("updated_at")
                for metadata in result.metadatas
            ]
        )

        if result.error is not None:
            upsert_cursor(
                store,
                CursorUpdatePayload(
                    source_type="github_org",
                    source_name=list_name,
                    source_key=result.key,
                    request_label=result.label,
                    request_query=result.request_query,
                    emitted_count=0,
                    status="error",
                    error_message=str(result.error),
                ),
            )
        else:
            upsert_cursor(
                store,
                CursorUpdatePayload(
                    source_type="github_org",
                    source_name=list_name,
                    source_key=result.key,
                    request_label=result.label,
                    request_query=result.request_query,
                    emitted_count=len(result.metadatas),
                    status="success",
                    last_max_pushed_at=max_pushed_at,
                    last_max_updated_at=max_updated_at,
                ),
            )

        if result.error is not None:
            logger.warning(
                "GitHub org seed ingestion failed for list=%s org=%s error=%s",
                list_name,
                result.label,
                str(result.error),
            )
            continue

        for metadata in result.metadatas:
            doc_id = f"org_repo:{list_name}:{metadata.source_item_id}"
            write_seed_item(
                store,
                doc_id=doc_id,
                source_type="org_repo",
                source_name=list_name,
                source_item_id=metadata.source_item_id,
                raw_metadata=metadata.raw_metadata,
                candidate_repo_urls=metadata.candidate_repo_urls,
            )

    _finalize_fetch_results(
        fetch_results,
        batch_type="github_org",
        source_name=list_name,
    )


def run_github_search_ingestion(
    list_name: str,
    search_queries: list[dict] | None = None,
) -> None:
    store = OpenSearchStore()

    if not search_queries:
        return

    keyed_queries: list[tuple[str, str, str, dict]] = []
    for search_query in search_queries:
        if not isinstance(search_query, dict):
            continue

        source_key, base_query = _build_search_request_key(search_query)
        if not base_query:
            continue

        effective_query = base_query
        if settings.seed_incremental_enabled:
            cursor = load_cursor(
                store,
                source_type="github_search",
                source_name=list_name,
                source_key=source_key,
            )
            if should_skip_by_min_interval(
                cursor,
                min_interval_minutes=settings.seed_incremental_min_interval_minutes,
            ):
                logger.info(
                    "Skip github search by incremental interval: list=%s key=%s query=%s",
                    list_name,
                    source_key,
                    base_query,
                )
                continue

            pushed_since_date = build_incremental_pushed_filter_date(
                cursor,
                overlap_days=settings.seed_incremental_overlap_days,
            )
            effective_query = _append_incremental_pushed_filter(
                base_query,
                pushed_since_date,
            )

        keyed_queries.append(
            (
                source_key,
                base_query,
                effective_query,
                {**search_query, "query": effective_query},
            )
        )

    if not keyed_queries:
        logger.info("No github search queries to request after incremental filtering: list=%s", list_name)
        return

    async def _run() -> list[GitHubSeedFetchResult]:
        async with GitHubApiAdapter(concurrency=settings.seed_github_global_concurrency) as api_adapter:
            adapter = GitHubSearchAdapter(api_adapter)
            return await _fetch_github_seed_batch(
                keyed_queries,
                lambda search_query: adapter.search_repositories(
                    list_name=list_name,
                    search_query=search_query,
                ),
            )

    fetch_results = asyncio.run(_run())

    for result in fetch_results:
        max_pushed_at = extract_max_timestamp(
            [
                (metadata.raw_metadata or {}).get("pushed_at")
                for metadata in result.metadatas
            ]
        )
        max_updated_at = extract_max_timestamp(
            [
                (metadata.raw_metadata or {}).get("updated_at")
                for metadata in result.metadatas
            ]
        )

        if result.error is not None:
            upsert_cursor(
                store,
                CursorUpdatePayload(
                    source_type="github_search",
                    source_name=list_name,
                    source_key=result.key,
                    request_label=result.label,
                    request_query=result.request_query,
                    emitted_count=0,
                    status="error",
                    error_message=str(result.error),
                ),
            )
        else:
            upsert_cursor(
                store,
                CursorUpdatePayload(
                    source_type="github_search",
                    source_name=list_name,
                    source_key=result.key,
                    request_label=result.label,
                    request_query=result.request_query,
                    emitted_count=len(result.metadatas),
                    status="success",
                    last_max_pushed_at=max_pushed_at,
                    last_max_updated_at=max_updated_at,
                ),
            )

        if result.error is not None:
            logger.warning(
                "GitHub search seed ingestion failed for list=%s query=%s error=%s",
                list_name,
                result.label,
                str(result.error),
            )
            continue

        query_digest = result.key

        for metadata in result.metadatas:
            doc_id = (
                f"github_search_repo:{list_name}:{query_digest}:"
                f"{safe_path_fragment(metadata.source_item_id)}"
            )
            write_seed_item(
                store,
                doc_id=doc_id,
                source_type="github_search_repo",
                source_name=list_name,
                source_item_id=metadata.source_item_id,
                raw_metadata=metadata.raw_metadata,
                candidate_repo_urls=metadata.candidate_repo_urls,
                extra_body={
                    "source_query": metadata.query,
                    "source_query_digest": query_digest,
                },
            )

    _finalize_fetch_results(
        fetch_results,
        batch_type="github_search",
        source_name=list_name,
    )


def run_github_topic_ingestion(
    list_name: str,
    topic_entries: list[dict] | None = None,
) -> None:
    store = OpenSearchStore()

    if not topic_entries:
        return

    keyed_topics: list[tuple[str, str, str, dict]] = []
    for topic_entry in topic_entries:
        if not isinstance(topic_entry, dict):
            continue

        source_key, topic_name = _build_topic_request_key(topic_entry)
        if not topic_name:
            continue

        effective_extra_query = _normalize_whitespace(str(topic_entry.get("query") or ""))
        if settings.seed_incremental_enabled:
            cursor = load_cursor(
                store,
                source_type="github_topic",
                source_name=list_name,
                source_key=source_key,
            )
            if should_skip_by_min_interval(
                cursor,
                min_interval_minutes=settings.seed_incremental_min_interval_minutes,
            ):
                logger.info(
                    "Skip github topic by incremental interval: list=%s key=%s topic=%s",
                    list_name,
                    source_key,
                    topic_name,
                )
                continue

            pushed_since_date = build_incremental_pushed_filter_date(
                cursor,
                overlap_days=settings.seed_incremental_overlap_days,
            )
            effective_extra_query = _append_incremental_pushed_filter(
                effective_extra_query,
                pushed_since_date,
            )

        requested_topic_entry = {**topic_entry, "query": effective_extra_query}
        requested_query_label = (
            f"topic:{topic_name} archived:false {effective_extra_query}".strip()
        )
        keyed_topics.append(
            (
                source_key,
                topic_name,
                requested_query_label,
                requested_topic_entry,
            )
        )

    if not keyed_topics:
        logger.info("No github topic entries to request after incremental filtering: list=%s", list_name)
        return

    async def _run() -> list[GitHubSeedFetchResult]:
        async with GitHubApiAdapter(concurrency=settings.seed_github_global_concurrency) as api_adapter:
            adapter = GitHubTopicAdapter(api_adapter)
            return await _fetch_github_seed_batch(
                keyed_topics,
                lambda topic_entry: adapter.search_topic_repositories(
                    list_name=list_name,
                    topic_entry=topic_entry,
                ),
            )

    fetch_results = asyncio.run(_run())

    for result in fetch_results:
        max_pushed_at = extract_max_timestamp(
            [
                (metadata.raw_metadata or {}).get("pushed_at")
                for metadata in result.metadatas
            ]
        )
        max_updated_at = extract_max_timestamp(
            [
                (metadata.raw_metadata or {}).get("updated_at")
                for metadata in result.metadatas
            ]
        )

        if result.error is not None:
            upsert_cursor(
                store,
                CursorUpdatePayload(
                    source_type="github_topic",
                    source_name=list_name,
                    source_key=result.key,
                    request_label=result.label,
                    request_query=result.request_query,
                    emitted_count=0,
                    status="error",
                    error_message=str(result.error),
                ),
            )
        else:
            upsert_cursor(
                store,
                CursorUpdatePayload(
                    source_type="github_topic",
                    source_name=list_name,
                    source_key=result.key,
                    request_label=result.label,
                    request_query=result.request_query,
                    emitted_count=len(result.metadatas),
                    status="success",
                    last_max_pushed_at=max_pushed_at,
                    last_max_updated_at=max_updated_at,
                ),
            )

        if result.error is not None:
            logger.warning(
                "GitHub topic seed ingestion failed for list=%s topic=%s error=%s",
                list_name,
                result.label,
                str(result.error),
            )
            continue

        topic_digest = result.key

        for metadata in result.metadatas:
            doc_id = (
                f"github_topic_repo:{list_name}:{topic_digest}:"
                f"{safe_path_fragment(metadata.source_item_id)}"
            )
            write_seed_item(
                store,
                doc_id=doc_id,
                source_type="github_topic_repo",
                source_name=list_name,
                source_item_id=metadata.source_item_id,
                raw_metadata=metadata.raw_metadata,
                candidate_repo_urls=metadata.candidate_repo_urls,
                extra_body={
                    "source_topic": metadata.topic,
                    "source_topic_digest": topic_digest,
                },
            )

    _finalize_fetch_results(
        fetch_results,
        batch_type="github_topic",
        source_name=list_name,
    )
