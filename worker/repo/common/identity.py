def build_github_repo_id(owner: str, repo_name: str) -> str:
    return f"github:{str(owner or '').strip().lower()}/{str(repo_name or '').strip().lower()}"
