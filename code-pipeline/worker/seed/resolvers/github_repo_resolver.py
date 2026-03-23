import requests
from worker.common.config import settings


class GitHubRepoResolver:
    BASE_URL = "https://api.github.com/repos"

    def __init__(self) -> None:
        self.headers = {"Accept": "application/vnd.github+json"}

    def fetch_repo_metadata(self, owner: str, repo: str) -> dict | None:
        response = requests.get(
            f"{self.BASE_URL}/{owner}/{repo}",
            headers=self.headers,
            timeout=settings.request_timeout_seconds,
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()
        return response.json()
