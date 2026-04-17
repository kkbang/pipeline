import asyncio
import logging
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
from worker.storage.opensearch_store import OpenSearchStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class GitHubSeedFetchResult:
    key: str
    label: str
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
    items: list[tuple[str, str, object]],
    fetcher,
) -> list[GitHubSeedFetchResult]:
    semaphore = asyncio.Semaphore(max(1, settings.seed_github_ingestion_concurrency))

    async def _fetch(key: str, label: str, payload: object) -> GitHubSeedFetchResult:
        async with semaphore:
            try:
                metadatas = await fetcher(payload)
                return GitHubSeedFetchResult(key=key, label=label, metadatas=metadatas)
            except Exception as exc:
                return GitHubSeedFetchResult(key=key, label=label, metadatas=[], error=exc)

    return await asyncio.gather(*(_fetch(key, label, payload) for key, label, payload in items))


def _raise_first_fetch_error(results: list[GitHubSeedFetchResult]) -> None:
    for result in results:
        if result.error is not None:
            raise result.error


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
        try:
            metadata = adapter.fetch_package_metadata(package_name)
        except Exception as exc:
            if first_error is None:
                first_error = exc
            logger.warning(
                "Package registry seed ingestion failed for registry=%s package=%s error=%s",
                registry,
                package_name,
                str(exc),
            )
            continue

        doc_id = f"{registry}:{package_name}"
        if settings.keep_full_package_registry_raw and metadata.full_raw_metadata is not None:
            store.upsert_document(
                collection_name="package_registry_full_metadata_index",
                doc_id=doc_id,
                body={
                    "registry_name": registry,
                    "package_name": package_name,
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

    async def _run() -> list[GitHubSeedFetchResult]:
        async with GitHubApiAdapter(concurrency=settings.seed_github_ingestion_concurrency) as api_adapter:
            adapter = GitHubOrgAdapter(api_adapter)
            return await _fetch_github_seed_batch(
                [(org_name, org_name, org_name) for org_name in org_names],
                lambda org_name: adapter.fetch_org_repositories(
                    list_name=list_name,
                    org_name=org_name,
                ),
            )

    fetch_results = asyncio.run(_run())

    for result in fetch_results:
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

    _raise_first_fetch_error(fetch_results)


def run_github_search_ingestion(
    list_name: str,
    search_queries: list[dict] | None = None,
) -> None:
    store = OpenSearchStore()

    if not search_queries:
        return

    keyed_queries = [
        (
            f"query_{index}",
            str(search_query.get("query") or "").strip(),
            search_query,
        )
        for index, search_query in enumerate(search_queries)
        if isinstance(search_query, dict) and str(search_query.get("query") or "").strip()
    ]
    async def _run() -> list[GitHubSeedFetchResult]:
        async with GitHubApiAdapter(concurrency=settings.seed_github_ingestion_concurrency) as api_adapter:
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
        if result.error is not None:
            logger.warning(
                "GitHub search seed ingestion failed for list=%s query=%s error=%s",
                list_name,
                result.label,
                str(result.error),
            )
            continue

        query_value = result.label
        query_digest = stable_digest(f"{result.key}:{query_value or list_name}")

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

    _raise_first_fetch_error(fetch_results)


def run_github_topic_ingestion(
    list_name: str,
    topic_entries: list[dict] | None = None,
) -> None:
    store = OpenSearchStore()

    if not topic_entries:
        return

    keyed_topics = [
        (
            f"topic_{index}",
            str(topic_entry.get("topic") or "").strip(),
            topic_entry,
        )
        for index, topic_entry in enumerate(topic_entries)
        if isinstance(topic_entry, dict) and str(topic_entry.get("topic") or "").strip()
    ]
    async def _run() -> list[GitHubSeedFetchResult]:
        async with GitHubApiAdapter(concurrency=settings.seed_github_ingestion_concurrency) as api_adapter:
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
        if result.error is not None:
            logger.warning(
                "GitHub topic seed ingestion failed for list=%s topic=%s error=%s",
                list_name,
                result.label,
                str(result.error),
            )
            continue

        topic_name = result.label
        topic_digest = stable_digest(f"{result.key}:{topic_name or list_name}")

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

    _raise_first_fetch_error(fetch_results)
