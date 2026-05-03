import re


def _clean_candidate(raw_value: str) -> str:
    value = raw_value.strip().strip("()[]{}<>'\"")
    value = value.rstrip(".,:;")
    value = value.split("#", 1)[0]
    value = value.split("?", 1)[0]

    if value.startswith("github.com/"):
        value = f"https://{value}"

    return value


def _unique_urls(urls: list[str]) -> list[str]:
    unique = []
    seen = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(url)
    return unique


URL_PATTERNS = [
    re.compile(r"(?:git\+)?https?://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:\.git)?"),
    re.compile(r"git@github\.com:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:\.git)?"),
    re.compile(r"github:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+"),
    re.compile(r"github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+"),
]


def extract_github_repo_candidate_urls(text: str) -> list[str]:
    if not isinstance(text, str) or not text.strip():
        return []

    urls = []
    for pattern in URL_PATTERNS:
        for match in pattern.findall(text):
            cleaned = _clean_candidate(match)
            if cleaned:
                urls.append(cleaned)

    return _unique_urls(urls)
