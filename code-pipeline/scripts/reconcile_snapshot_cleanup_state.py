#!/usr/bin/env python3
import argparse
import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib import parse, request


RAW_REPO_SNAPSHOT_DIR = "raw/repo_snapshot"
RAW_REPO_SNAPSHOT_DOWNLOAD_DIR = "raw/repo_snapshot_download"


@dataclass
class OpenSearchConfig:
    base_url: str
    auth_header: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize repo_registry_index snapshot cleanup metadata when local artifacts are already missing."
    )
    parser.add_argument(
        "--root",
        default=str(Path.home() / "pipeline" / "code-pipeline"),
        help="Project root that contains .env and local_data/",
    )
    parser.add_argument(
        "--index",
        default="repo_registry_index",
        help="OpenSearch index name to reconcile",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Search page size",
    )
    parser.add_argument(
        "--from-status",
        default="skipped",
        help="Current snapshot_local_cleanup_status to target",
    )
    parser.add_argument(
        "--from-error",
        default="snapshot_artifacts_missing",
        help="Current snapshot_local_cleanup_error to target",
    )
    parser.add_argument(
        "--to-status",
        default="missing_confirmed",
        help="New cleanup status to write when files are confirmed missing",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates. Without this flag, runs in dry-run mode.",
    )
    return parser.parse_args()


def load_env(env_path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def build_opensearch_config(env: dict[str, str]) -> OpenSearchConfig:
    host = env.get("OPENSEARCH_HOST", "127.0.0.1")
    port = env.get("OPENSEARCH_PORT", "9200")
    scheme = "https" if env.get("OPENSEARCH_USE_SSL", "false").lower() == "true" else "http"
    auth_header = None

    user = env.get("OPENSEARCH_USER", "")
    password = env.get("OPENSEARCH_PASSWORD", "")
    if user:
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("utf-8")
        auth_header = f"Basic {token}"

    return OpenSearchConfig(base_url=f"{scheme}://{host}:{port}", auth_header=auth_header)


def os_post(config: OpenSearchConfig, path: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if config.auth_header:
        headers["Authorization"] = config.auth_header
    req = request.Request(
        f"{config.base_url}{path}",
        data=data,
        headers=headers,
        method="POST",
    )
    with request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def resolve_expected_snapshot_paths(local_data_dir: Path, owner: str, repo: str) -> tuple[Path, Path]:
    extract_dir = local_data_dir / RAW_REPO_SNAPSHOT_DIR / owner / repo
    download_path = local_data_dir / RAW_REPO_SNAPSHOT_DOWNLOAD_DIR / owner / repo / "snapshot.tar.gz"
    return extract_dir.resolve(), download_path.resolve()


def iter_target_docs(
    config: OpenSearchConfig,
    *,
    index_name: str,
    page_size: int,
    from_status: str,
    from_error: str,
):
    search_after = None
    while True:
        query = {
            "size": page_size,
            "sort": [{"_id": "asc"}],
            "_source": [
                "owner",
                "repo_name",
                "snapshot_local_cleanup_status",
                "snapshot_local_cleanup_error",
                "snapshot_extract_dir",
                "snapshot_download_path",
                "snapshot_root_path",
                "chunk_status",
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {"term": {"snapshot_local_cleanup_status.keyword": from_status}},
                                    {"term": {"snapshot_local_cleanup_status": from_status}},
                                ],
                                "minimum_should_match": 1,
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {"term": {"snapshot_local_cleanup_error.keyword": from_error}},
                                    {"term": {"snapshot_local_cleanup_error": from_error}},
                                ],
                                "minimum_should_match": 1,
                            }
                        },
                    ]
                }
            },
        }
        if search_after is not None:
            query["search_after"] = search_after

        result = os_post(config, f"/{index_name}/_search", query)
        hits = result.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            yield hit

        search_after = hits[-1].get("sort")
        if len(hits) < page_size:
            break


def update_doc(
    config: OpenSearchConfig,
    *,
    index_name: str,
    doc_id: str,
    new_status: str,
) -> dict:
    payload = {
        "doc": {
            "snapshot_extract_dir": None,
            "snapshot_download_path": None,
            "snapshot_root_path": None,
            "snapshot_local_cleanup_status": new_status,
            "snapshot_local_cleanup_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_local_cleanup_error": None,
        }
    }
    return os_post(config, f"/{index_name}/_update/{parse.quote(doc_id, safe='')}", payload)


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    env = load_env(root / ".env")
    config = build_opensearch_config(env)
    local_data_dir = Path(env.get("LOCAL_DATA_DIR", str(root / "local_data")))
    if not local_data_dir.is_absolute():
        local_data_dir = (root / local_data_dir).resolve()

    seen = 0
    updated = 0
    present = 0
    unsafe = 0
    failed = 0

    for hit in iter_target_docs(
        config,
        index_name=args.index,
        page_size=max(1, int(args.page_size)),
        from_status=args.from_status,
        from_error=args.from_error,
    ):
        seen += 1
        doc_id = hit["_id"]
        source = hit.get("_source") or {}
        owner = str(source.get("owner") or "").strip().lower()
        repo = str(source.get("repo_name") or "").strip().lower()

        if not owner or not repo:
            print(f"[skip-missing-owner-repo] {doc_id}")
            continue

        extract_dir, download_path = resolve_expected_snapshot_paths(local_data_dir, owner, repo)
        if not is_within(extract_dir, local_data_dir) or not is_within(download_path, local_data_dir):
            unsafe += 1
            print(f"[skip-unsafe] {doc_id}: {extract_dir} | {download_path}")
            continue

        if extract_dir.exists() or download_path.exists():
            present += 1
            print(f"[still-present] {doc_id}: {extract_dir.exists()} {download_path.exists()}")
            continue

        if not args.apply:
            updated += 1
            print(f"[dry-run-update] {doc_id}")
            continue

        try:
            update_doc(
                config,
                index_name=args.index,
                doc_id=doc_id,
                new_status=args.to_status,
            )
            updated += 1
            print(f"[updated] {doc_id}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"[failed] {doc_id}: {exc}")

    print()
    print(f"seen={seen} updated={updated} present={present} unsafe={unsafe} failed={failed}")
    print(f"mode={'apply' if args.apply else 'dry-run'}")


if __name__ == "__main__":
    main()
