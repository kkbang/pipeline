def qualify_repo(repo_metadata: dict | None) -> tuple[bool, str, float]:
    if not repo_metadata:
        return False, "repo_not_found", 0.0

    if repo_metadata.get("private", False):
        return False, "private_repo", 0.0

    if repo_metadata.get("archived", False):
        return False, "archived_repo", 0.0

    score = 0.0

    if repo_metadata.get("license"):
        score += 0.35

    if not repo_metadata.get("fork", False):
        score += 0.20

    if repo_metadata.get("default_branch"):
        score += 0.15

    stars = repo_metadata.get("stargazers_count", 0)
    if stars >= 1000:
        score += 0.20
    elif stars >= 100:
        score += 0.10
    elif stars >= 10:
        score += 0.05

    if repo_metadata.get("updated_at"):
        score += 0.10

    return True, "qualified", round(score, 4)