"""Idempotently create the primary originals bucket using mounted credentials."""
import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from reconcilex_config import secret_value


def ensure_bucket(client, bucket):
    try:
        client.head_bucket(Bucket=bucket)
        return False
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if status != 404 and code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
    client.create_bucket(Bucket=bucket)
    return True


def main():
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=secret_value("S3_ACCESS_KEY", required=True),
        aws_secret_access_key=secret_value("S3_SECRET_KEY", required=True),
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    bucket = os.getenv("S3_BUCKET", "reconcilex")
    created = ensure_bucket(client, bucket)
    print({"event": "storage_bucket_ready", "bucket": bucket, "created": created})


if __name__ == "__main__":
    main()
