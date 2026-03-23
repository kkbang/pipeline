import json
import boto3
from worker.common.config import settings


class S3Store:
    def __init__(self) -> None:
        self.client = boto3.client("s3", region_name=settings.aws_region)
        self.bucket = settings.s3_bucket

    def put_json(self, key: str, payload: dict) -> str:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        return key

    def get_json(self, key: str) -> dict:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))