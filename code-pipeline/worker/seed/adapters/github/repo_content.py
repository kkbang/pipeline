import base64

from .api import GitHubApiAdapter


class GitHubRepoContentAdapter:
    def __init__(self, api_adapter: GitHubApiAdapter) -> None:
        self.api_adapter = api_adapter

    def _decode_content(self, payload: dict) -> str | None:
        content = payload.get("content")
        encoding = payload.get("encoding")
        if not isinstance(content, str) or encoding != "base64":
            return None

        try:
            decoded = base64.b64decode(content.encode("utf-8"))
            return decoded.decode("utf-8")
        except Exception:
            return None

    async def fetch_readme_text(self, owner: str, repo: str) -> str | None:
        payload = await self.api_adapter.get_json(
            f"/repos/{owner}/{repo}/readme",
            not_found_none=True,
        )
        if payload is None:
            return None

        return self._decode_content(payload)

    async def fetch_file_text(self, owner: str, repo: str, path: str) -> str | None:
        payload = await self.api_adapter.get_json(
            f"/repos/{owner}/{repo}/contents/{path}",
            not_found_none=True,
        )
        if payload is None:
            return None

        return self._decode_content(payload)
