import os
from dataclasses import dataclass


@dataclass
class Settings:
    app_env: str = os.getenv("APP_ENV", "local")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    aws_region: str = os.getenv("AWS_REGION", "ap-northeast-2")
    s3_bucket: str = os.getenv("S3_BUCKET", "")

    opensearch_host: str = os.getenv("OPENSEARCH_HOST", "")
    opensearch_port: int = int(os.getenv("OPENSEARCH_PORT", "9200"))
    opensearch_user: str = os.getenv("OPENSEARCH_USER", "")
    opensearch_password: str = os.getenv("OPENSEARCH_PASSWORD", "")
    opensearch_use_ssl: bool = os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true"

    github_token: str = os.getenv("GITHUB_TOKEN", "")
    request_timeout_seconds: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    keep_full_package_registry_raw: bool = (
        os.getenv("KEEP_FULL_PACKAGE_REGISTRY_RAW", "false").lower() == "true"
    )


settings = Settings()
