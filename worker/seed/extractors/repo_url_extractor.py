def _unique_urls(urls: list[str]) -> list[str]:
    unique = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique.append(url)

    return unique


def _append_if_non_empty(urls: list[str], value: object) -> None:
    if isinstance(value, str) and value.strip():
        urls.append(value.strip())


def extract_repo_candidates_from_pypi(raw_metadata: dict) -> list[str]:
    info = raw_metadata.get("info", {})
    urls = []

    project_urls = info.get("project_urls") or {}
    for value in project_urls.values():
        if isinstance(value, str) and value.strip():
            urls.append(value.strip())

    home_page = info.get("home_page")
    if isinstance(home_page, str) and home_page.strip():
        urls.append(home_page.strip())

    return _unique_urls(urls)


def extract_repo_candidates_from_npm(raw_metadata: dict) -> list[str]:
    urls = []

    latest_version = ((raw_metadata.get("dist-tags") or {}).get("latest"))
    latest_metadata = ((raw_metadata.get("versions") or {}).get(latest_version) or {})

    for payload in [latest_metadata, raw_metadata]:
        repository = payload.get("repository")
        if isinstance(repository, dict):
            repository_url = repository.get("url")
            if isinstance(repository_url, str) and repository_url.strip():
                urls.append(repository_url.strip())
        elif isinstance(repository, str) and repository.strip():
            urls.append(repository.strip())

        homepage = payload.get("homepage")
        if isinstance(homepage, str) and homepage.strip():
            urls.append(homepage.strip())

        bugs = payload.get("bugs")
        if isinstance(bugs, dict):
            bug_url = bugs.get("url")
            if isinstance(bug_url, str) and bug_url.strip():
                urls.append(bug_url.strip())
        elif isinstance(bugs, str) and bugs.strip():
            urls.append(bugs.strip())

    return _unique_urls(urls)


def extract_repo_candidates_from_cratesio(raw_metadata: dict) -> list[str]:
    urls = []
    crate = raw_metadata.get("crate") or {}

    for key in ["repository", "homepage", "documentation"]:
        _append_if_non_empty(urls, crate.get(key))

    return _unique_urls(urls)


def extract_repo_candidates_from_rubygems(raw_metadata: dict) -> list[str]:
    urls = []

    for key in [
        "source_code_uri",
        "homepage_uri",
        "bug_tracker_uri",
        "documentation_uri",
        "wiki_uri",
    ]:
        _append_if_non_empty(urls, raw_metadata.get(key))

    return _unique_urls(urls)


def extract_repo_candidates_from_packagist(raw_metadata: dict) -> list[str]:
    urls = []
    packages = raw_metadata.get("packages") or {}

    for versions in packages.values():
        if not isinstance(versions, list):
            continue
        for version in versions:
            if not isinstance(version, dict):
                continue

            _append_if_non_empty(urls, version.get("homepage"))

            source = version.get("source")
            if isinstance(source, dict):
                source_url = source.get("url")
                _append_if_non_empty(urls, source_url)

            support = version.get("support")
            if isinstance(support, dict):
                for support_key in ["source", "issues", "docs", "forum"]:
                    support_url = support.get(support_key)
                    _append_if_non_empty(urls, support_url)

    return _unique_urls(urls)


def extract_repo_candidates_from_pubdev(raw_metadata: dict) -> list[str]:
    urls = []
    latest = raw_metadata.get("latest") or {}
    pubspec = latest.get("pubspec") or {}

    for key in ["repository", "homepage", "issue_tracker", "documentation"]:
        _append_if_non_empty(urls, pubspec.get(key))

    return _unique_urls(urls)


def extract_repo_candidates_from_nuget(raw_metadata: dict) -> list[str]:
    urls = []
    latest = raw_metadata.get("latest_catalog_entry") or {}
    for key in ["projectUrl", "repositoryUrl", "licenseUrl", "iconUrl"]:
        _append_if_non_empty(urls, latest.get(key))

    metadata_url = raw_metadata.get("source_reference_url")
    _append_if_non_empty(urls, metadata_url)
    return _unique_urls(urls)


def extract_repo_candidates_from_maven(raw_metadata: dict) -> list[str]:
    urls = []
    latest_doc = raw_metadata.get("latest_doc") or {}
    pom = raw_metadata.get("latest_pom") or {}
    for key in ["url", "homepage", "repository", "documentation"]:
        _append_if_non_empty(urls, latest_doc.get(key))
    for key in ["project_url", "scm_url", "scm_connection", "scm_developer_connection", "issue_management_url"]:
        _append_if_non_empty(urls, pom.get(key))
    return _unique_urls(urls)


def extract_repo_candidates_from_hexpm(raw_metadata: dict) -> list[str]:
    urls = []
    meta = raw_metadata.get("meta") or {}
    links = meta.get("links") or {}
    if isinstance(links, dict):
        for value in links.values():
            _append_if_non_empty(urls, value)
    for key in ["html_url", "docs_html_url"]:
        _append_if_non_empty(urls, raw_metadata.get(key))
    return _unique_urls(urls)


def extract_repo_candidates_from_cpan(raw_metadata: dict) -> list[str]:
    urls = []
    resources = raw_metadata.get("resources") or {}
    if isinstance(resources, dict):
        _append_if_non_empty(urls, resources.get("homepage"))

        repository = resources.get("repository") or {}
        if isinstance(repository, dict):
            for key in ["url", "web"]:
                _append_if_non_empty(urls, repository.get(key))

        bugtracker = resources.get("bugtracker") or {}
        if isinstance(bugtracker, dict):
            for key in ["web", "mailto"]:
                _append_if_non_empty(urls, bugtracker.get(key))

    _append_if_non_empty(urls, raw_metadata.get("download_url"))
    return _unique_urls(urls)


def extract_repo_candidates_from_hackage(raw_metadata: dict) -> list[str]:
    urls = []
    fields = raw_metadata.get("cabal_fields") or {}
    if isinstance(fields, dict):
        for key in ["homepage", "bug_reports", "source_repository"]:
            _append_if_non_empty(urls, fields.get(key))

    _append_if_non_empty(urls, raw_metadata.get("source_reference_url"))
    return _unique_urls(urls)


def extract_repo_candidates_from_cocoapods(raw_metadata: dict) -> list[str]:
    urls = []
    latest = raw_metadata.get("latest_spec") or {}
    if isinstance(latest, dict):
        _append_if_non_empty(urls, latest.get("homepage"))
        source = latest.get("source") or {}
        if isinstance(source, dict):
            for value in source.values():
                _append_if_non_empty(urls, value)

    for key in ["page_url", "json_url"]:
        _append_if_non_empty(urls, raw_metadata.get(key))

    return _unique_urls(urls)


def extract_repo_candidates_from_gomod(raw_metadata: dict) -> list[str]:
    urls = []
    module_path = str(raw_metadata.get("module_path") or "").strip()
    if module_path:
        parts = module_path.split("/")
        if len(parts) >= 3 and "." in parts[0]:
            _append_if_non_empty(urls, f"https://{parts[0]}/{parts[1]}/{parts[2]}")
        _append_if_non_empty(urls, f"https://pkg.go.dev/{module_path}")
    return _unique_urls(urls)


def extract_repo_candidates_from_swiftpm(raw_metadata: dict) -> list[str]:
    urls = []
    _append_if_non_empty(urls, raw_metadata.get("repository_url"))
    _append_if_non_empty(urls, raw_metadata.get("source_reference_url"))
    return _unique_urls(urls)
