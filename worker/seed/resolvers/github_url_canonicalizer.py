import re
from urllib.parse import urlparse


_ALLOWED_GITHUB_HOSTS = {"github.com", "www.github.com"}
_OWNER_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})$")
_REPO_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,100}$")


def _normalize_git_style(raw_url: str) -> str:
    raw_url = raw_url.strip()

    if raw_url.startswith("github:"):
        raw_url = raw_url.replace("github:", "https://github.com/", 1)

    if raw_url.startswith("git+"):
        raw_url = raw_url[4:]

    if raw_url.startswith("git@github.com:"):
        raw_url = raw_url.replace("git@github.com:", "https://github.com/")

    if raw_url.startswith("ssh://git@github.com/"):
        raw_url = raw_url.replace("ssh://git@github.com/", "https://github.com/")

    if raw_url.startswith("git://github.com/"):
        raw_url = raw_url.replace("git://github.com/", "https://github.com/")

    return raw_url


def canonicalize_github_repo_url(raw_url: str) -> tuple[str, str, str] | None:
    if not raw_url:
        return None

    url = _normalize_git_style(raw_url)
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").strip().lower()
    scheme = (parsed.scheme or "").strip().lower()

    if hostname not in _ALLOWED_GITHUB_HOSTS:
        return None

    if scheme and scheme not in {"http", "https"}:
        return None

    path = parsed.path.strip("/")
    path = re.sub(r"\.git$", "", path)

    parts = path.split("/")
    if len(parts) < 2:
        return None

    owner, repo = parts[0], parts[1]
    if not owner or not repo:
        return None
    if not _OWNER_PATTERN.fullmatch(owner):
        return None
    if not _REPO_PATTERN.fullmatch(repo):
        return None

    return owner, repo, f"https://github.com/{owner}/{repo}"
