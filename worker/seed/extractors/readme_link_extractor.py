from worker.seed.extractors.github_repo_reference_extractor import (
    extract_github_repo_candidate_urls,
)


def extract_repo_candidates_from_readme(readme_text: str) -> list[str]:
    return extract_github_repo_candidate_urls(readme_text)
