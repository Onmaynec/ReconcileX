"""S3-compatible object storage for original import files."""
import os
from pathlib import Path
import boto3
from botocore.client import Config


def _client():
    return boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def put_original(key, data):
    if os.getenv("STORAGE_MODE") == "local":
        root = Path(os.getenv("LOCAL_STORAGE_DIR", "./demo-output/originals")).resolve()
        path = (root / key).resolve()
        if not path.is_relative_to(root):
            raise ValueError("Invalid object key")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return
    client = _client()
    extra = {}
    if os.getenv("S3_SSE") == "AES256":
        extra["ServerSideEncryption"] = "AES256"
    client.put_object(Bucket=os.environ["S3_BUCKET"], Key=key, Body=data, **extra)


def get_original(key):
    if os.getenv("STORAGE_MODE") == "local":
        root = Path(os.getenv("LOCAL_STORAGE_DIR", "./demo-output/originals")).resolve()
        path = (root / key).resolve()
        if not path.is_relative_to(root):
            raise ValueError("Invalid object key")
        return path.read_bytes()
    return _client().get_object(Bucket=os.environ["S3_BUCKET"], Key=key)["Body"].read()
