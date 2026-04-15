import json
import re
from pathlib import Path
from urllib.parse import quote

import boto3
from botocore.config import Config

from worker.common.config import settings


class B2Store:
    def __init__(self) -> None:
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.b2_endpoint_url or None,
            region_name=settings.b2_region,
            aws_access_key_id=settings.b2_key_id or None,
            aws_secret_access_key=settings.b2_application_key or None,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        self.bucket = settings.b2_bucket

    def _build_document_key(self, collection_name: str, doc_id: str) -> str:
        safe_doc_id = doc_id.replace(":", "__").replace("/", "__")
        safe_doc_id = re.sub(r"[^A-Za-z0-9._-]", "_", safe_doc_id)
        return f"{collection_name}/{safe_doc_id}.json"

    def upsert_document(self, collection_name: str, doc_id: str, body: dict) -> None:
        key = self._build_document_key(collection_name, doc_id)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps({"_id": doc_id, "_source": body}, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

    def get_document(self, collection_name: str, doc_id: str) -> dict:
        key = self._build_document_key(collection_name, doc_id)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))

    def upload_file(
        self,
        local_path: str | Path,
        key: str,
        *,
        content_type: str | None = None,
    ) -> None:
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type

        if extra_args:
            self.client.upload_file(
                Filename=str(local_path),
                Bucket=self.bucket,
                Key=key,
                ExtraArgs=extra_args,
            )
            return

        self.client.upload_file(
            Filename=str(local_path),
            Bucket=self.bucket,
            Key=key,
        )

    def build_object_url(self, key: str) -> str:
        endpoint = (settings.b2_endpoint_url or "").rstrip("/")
        if not endpoint:
            return ""
        return f"{endpoint}/{self.bucket}/{quote(key, safe='/')}"
