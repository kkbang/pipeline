from urllib.parse import urlparse
import re


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

    if "github.com" not in parsed.netloc:
        return None

    path = parsed.path.strip("/")
    path = re.sub(r"\.git$", "", path)

    parts = path.split("/")
    if len(parts) < 2:
        return None

    owner, repo = parts[0], parts[1]
    if not owner or not repo:
        return None

    return owner, repo, f"https://github.com/{owner}/{repo}"
