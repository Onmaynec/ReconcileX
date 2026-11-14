"""Optional off-host, encrypted, append-only S3 snapshot replication.

This module never lists or deletes source originals and never restores into
the live database. Completed manifests are written only after every object
has been uploaded and read back with a matching SHA-256.
"""
import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

SNAPSHOT_NAME = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")
ORIGINAL_KEY = re.compile(r"^(?:originals|rejected)/[0-9a-f-]{36}/[0-9a-f]{64}$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")
PREFIX = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9/_-]{0,80}$")


@dataclass(frozen=True)
class Settings:
    endpoint: str
    bucket: str
    access_key: str
    secret_key: str
    region: str
    prefix: str
    encryption: str
    kms_key: str | None


def configured():
    value = os.getenv("OFFSITE_ENABLED", "0").strip()
    if value not in {"0", "1"}:
        raise RuntimeError("OFFSITE_ENABLED must be 0 or 1")
    return value == "1"


def settings():
    if not configured():
        raise RuntimeError("Off-site replication is disabled")
    required = (
        "OFFSITE_S3_ENDPOINT", "OFFSITE_S3_BUCKET",
        "OFFSITE_S3_ACCESS_KEY", "OFFSITE_S3_SECRET_KEY",
    )
    if any(not os.getenv(key) for key in required):
        raise RuntimeError("Off-site S3 endpoint, bucket and credentials are mandatory")
    endpoint = os.environ["OFFSITE_S3_ENDPOINT"].rstrip("/")
    parsed = urlparse(endpoint)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise RuntimeError("Off-site endpoint must be an explicit HTTPS URL without credentials")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise RuntimeError("Off-site endpoint must be a bare HTTPS origin")
    source_endpoint = os.getenv("S3_ENDPOINT", "").rstrip("/")
    if endpoint == source_endpoint or (
        source_endpoint and urlparse(source_endpoint).hostname == parsed.hostname
    ):
        raise RuntimeError("Off-site storage must use an independent endpoint")
    access = os.environ["OFFSITE_S3_ACCESS_KEY"]
    secret = os.environ["OFFSITE_S3_SECRET_KEY"]
    if access == os.getenv("S3_ACCESS_KEY") or secret == os.getenv("S3_SECRET_KEY"):
        raise RuntimeError("Off-site storage must use separate credentials")
    prefix = os.getenv("OFFSITE_S3_PREFIX", "reconcilex").strip("/")
    if not PREFIX.fullmatch(prefix) or "//" in prefix or "/../" in f"/{prefix}/":
        raise RuntimeError("Invalid off-site storage prefix")
    encryption = os.getenv("OFFSITE_S3_ENCRYPTION", "AES256")
    if encryption not in {"AES256", "aws:kms"}:
        raise RuntimeError("Off-site server-side encryption is mandatory")
    kms = os.getenv("OFFSITE_S3_KMS_KEY_ID") or None
    if encryption == "aws:kms" and not kms:
        raise RuntimeError("OFFSITE_S3_KMS_KEY_ID required for KMS encryption")
    return Settings(
        endpoint=endpoint, bucket=os.environ["OFFSITE_S3_BUCKET"],
        access_key=access, secret_key=secret,
        region=os.getenv("OFFSITE_S3_REGION", "us-east-1"),
        prefix=prefix, encryption=encryption, kms_key=kms,
    )


def client_for(config):
    return boto3.client(
        "s3", endpoint_url=config.endpoint, region_name=config.region,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
    )


def key_for(config, name, relative):
    if not SNAPSHOT_NAME.fullmatch(name):
        raise RuntimeError("Invalid backup snapshot name")
    if relative == "database.dump" or relative == "manifest.json":
        pass
    elif relative.startswith("objects/") and ORIGINAL_KEY.fullmatch(relative[8:]):
        pass
    else:
        raise RuntimeError("Unsafe off-site snapshot file path")
    return f"{config.prefix}/snapshots/{name}/{relative}"


def validate_manifest(manifest):
    if not isinstance(manifest, dict) or manifest.get("schema") != 1:
        raise RuntimeError("Unsupported off-site manifest schema")
    db = manifest.get("database")
    if not isinstance(db, dict) or db.get("file") != "database.dump":
        raise RuntimeError("Missing database dump descriptor")
    if not isinstance(db.get("bytes"), int) or db["bytes"] <= 0 or not DIGEST.fullmatch(str(db.get("sha256", ""))):
        raise RuntimeError("Invalid database dump checksum or size")
    objects = manifest.get("objects")
    if not isinstance(objects, list) or not isinstance(manifest.get("bucket"), str):
        raise RuntimeError("Invalid object manifest")
    try:
        when = datetime.fromisoformat(manifest["created_utc"])
        if when.tzinfo is None or when.utcoffset() is None:
            raise ValueError("Timezone missing")
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Invalid manifest creation timestamp") from exc
    seen = set()
    for item in objects:
        if not isinstance(item, dict):
            raise RuntimeError("Invalid object descriptor")
        key = item.get("key")
        if not isinstance(key, str) or not ORIGINAL_KEY.fullmatch(key) or key in seen:
            raise RuntimeError("Unsafe or duplicated object key")
        seen.add(key)
        if (not isinstance(item.get("bytes"), int) or item["bytes"] < 0
                or not DIGEST.fullmatch(str(item.get("sha256", "")))
                or item["sha256"] != key.rsplit("/", 1)[-1]):
            raise RuntimeError("Invalid original object length or checksum")
    return manifest


def object_descriptors(manifest):
    return [("database.dump", manifest["database"]["bytes"],
             manifest["database"]["sha256"])] + [
        ("objects/" + item["key"], item["bytes"], item["sha256"])
        for item in manifest["objects"]
    ]


def sha256_file(path):
    hashvalue = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hashvalue.update(chunk)
    return hashvalue.hexdigest()


def head_or_none(client, config, key):
    try:
        return client.head_object(Bucket=config.bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def require_encryption(head, config):
    if head.get("ServerSideEncryption") != config.encryption:
        raise RuntimeError("Off-site object is missing required server-side encryption")
    if config.encryption == "aws:kms" and head.get("SSEKMSKeyId") != config.kms_key:
        raise RuntimeError("Off-site object uses a different KMS key")


def stream_object(client, config, key, destination=None, max_bytes=None):
    response = client.get_object(Bucket=config.bucket, Key=key)
    body = response["Body"]
    hashed = hashlib.sha256()
    size = 0
    stream = None
    try:
        if destination is not None:
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            stream = destination.open("xb")
            destination.chmod(0o600)
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if max_bytes is not None and size > max_bytes:
                raise RuntimeError("Remote object exceeds allowed size")
            hashed.update(chunk)
            if stream is not None:
                stream.write(chunk)
        if stream is not None:
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        if stream is not None:
            stream.close()
        body.close()
    return size, hashed.hexdigest()


def read_manifest(client, config, name):
    key = key_for(config, name, "manifest.json")
    meta = head_or_none(client, config, key)
    if meta is None:
        raise RuntimeError("Off-site snapshot has no completed manifest")
    require_encryption(meta, config)
    if meta.get("ContentLength", 0) > 20 * 1024 * 1024:
        raise RuntimeError("Off-site manifest is too large")
    response = client.get_object(Bucket=config.bucket, Key=key)
    try:
        raw = response["Body"].read(20 * 1024 * 1024 + 1)
    finally:
        response["Body"].close()
    if len(raw) > 20 * 1024 * 1024 or len(raw) != meta["ContentLength"]:
        raise RuntimeError("Off-site manifest size mismatch")
    if hashlib.sha256(raw).hexdigest() != meta.get("Metadata", {}).get("sha256"):
        raise RuntimeError("Off-site manifest checksum mismatch")
    try:
        return validate_manifest(json.loads(raw))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Invalid remote manifest JSON") from exc


def verify_snapshot(name, expected=None, full=True, client=None, config=None):
    config = config or settings()
    client = client or client_for(config)
    manifest = read_manifest(client, config, name)
    if expected is not None and manifest != validate_manifest(expected):
        raise RuntimeError("Off-site manifest does not match local snapshot")
    if full:
        for relative, length, digest in object_descriptors(manifest):
            key = key_for(config, name, relative)
            meta = head_or_none(client, config, key)
            if meta is None or meta.get("ContentLength") != length:
                raise RuntimeError("Missing or truncated off-site backup object")
            require_encryption(meta, config)
            size, actual = stream_object(client, config, key)
            if size != length or actual != digest:
                raise RuntimeError("Off-site snapshot object checksum mismatch")
    return manifest


def encryption_args(config):
    values = {"ServerSideEncryption": config.encryption}
    if config.encryption == "aws:kms":
        values["SSEKMSKeyId"] = config.kms_key
    return values


def upload_snapshot(snapshot, manifest, client=None, config=None):
    config = config or settings()
    client = client or client_for(config)
    manifest = validate_manifest(manifest)
    name = snapshot.name
    raw = (snapshot / "manifest.json").read_bytes()
    if json.loads(raw) != manifest:
        raise RuntimeError("Local manifest was modified during transfer")
    commit_key = key_for(config, name, "manifest.json")
    if head_or_none(client, config, commit_key) is not None:
        verify_snapshot(name, expected=manifest, full=True, client=client, config=config)
        return False
    for relative, length, digest in object_descriptors(manifest):
        path = snapshot / relative
        if not path.is_file() or path.stat().st_size != length or sha256_file(path) != digest:
            raise RuntimeError("Local backup changed during off-site upload")
        key = key_for(config, name, relative)
        if head_or_none(client, config, key) is None:
            client.upload_file(
                str(path), config.bucket, key,
                ExtraArgs={**encryption_args(config), "Metadata": {"sha256": digest}},
            )
        meta = head_or_none(client, config, key)
        if meta is None or meta.get("ContentLength") != length:
            raise RuntimeError("Incomplete off-site upload")
        require_encryption(meta, config)
        size, actual = stream_object(client, config, key)
        if size != length or actual != digest:
            raise RuntimeError("Off-site object failed upload read-back verification")
    manifest_digest = hashlib.sha256(raw).hexdigest()
    try:
        client.put_object(
            Bucket=config.bucket, Key=commit_key, Body=raw,
            IfNoneMatch="*", Metadata={"sha256": manifest_digest},
            **encryption_args(config),
        )
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code", "")) not in {
            "412", "PreconditionFailed", "ConditionalRequestConflict",
        }:
            raise
        # If another process published this name, exact content must match.
    verify_snapshot(name, expected=manifest, full=True, client=client, config=config)
    return True


def download_snapshot(name, destination, client=None, config=None):
    """Download to a NEW private directory only; never overwrite local data."""
    config = config or settings()
    client = client or client_for(config)
    manifest = verify_snapshot(name, full=False, client=client, config=config)
    destination = Path(destination)
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    try:
        for relative, length, digest in object_descriptors(manifest):
            target = destination / relative
            key = key_for(config, name, relative)
            size, actual = stream_object(client, config, key, target)
            if size != length or actual != digest:
                raise RuntimeError("Downloaded off-site object checksum mismatch")
        manifest_key = key_for(config, name, "manifest.json")
        response = client.get_object(Bucket=config.bucket, Key=manifest_key)
        try:
            raw = response["Body"].read(20 * 1024 * 1024 + 1)
        finally:
            response["Body"].close()
        if len(raw) > 20 * 1024 * 1024 or json.loads(raw) != manifest:
            raise RuntimeError("Downloaded off-site manifest differs")
        (destination / "manifest.json").write_bytes(raw)
        return manifest
    except Exception:
        shutil.rmtree(destination)
        raise


def latest_snapshot_name(client=None, config=None):
    config = config or settings()
    client = client or client_for(config)
    prefix = f"{config.prefix}/snapshots/"
    names = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=config.bucket, Prefix=prefix,
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/manifest.json"):
                continue
            name = key[len(prefix):-len("/manifest.json")]
            if SNAPSHOT_NAME.fullmatch(name):
                names.append(name)
    if not names:
        raise RuntimeError("No completed off-site snapshots available")
    return max(names)


def check_recent(local_snapshot, hours, client=None, config=None):
    if hours <= 0:
        raise RuntimeError("Off-site maximum age must be positive")
    config = config or settings()
    client = client or client_for(config)
    manifest = json.loads((local_snapshot / "manifest.json").read_text(encoding="utf-8"))
    verified = verify_snapshot(
        local_snapshot.name, expected=manifest, full=False,
        client=client, config=config,
    )
    created = datetime.fromisoformat(verified["created_utc"])
    age = (datetime.now(timezone.utc) - created).total_seconds() / 3600
    if not 0 <= age <= hours:
        raise RuntimeError(f"Off-site snapshot is stale ({age:.2f}h)")
    return {"age_hours": round(age, 3), "snapshot": local_snapshot.name}
