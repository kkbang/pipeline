def _unique_urls(urls: list[str]) -> list[str]:
    unique = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique.append(url)

    return unique


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
