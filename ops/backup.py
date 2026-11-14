"""ReconcileX v0.2: consistent PostgreSQL-first, S3-second backup and restore verification.

The source originals are content-addressed and never deleted by ReconcileX:
copying objects *after* pg_dump ensures that records present in the dump have
their referenced originals in the same backup (barring external deletion).
Run only with the dedicated backup container and a protected backup volume.
"""
import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

KEY_PATTERN = re.compile(
    r"^(?:originals|rejected)/[0-9a-f-]{36}/[0-9a-f]{64}$")
DIR_PATTERN = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")
BACKUP_ROOT = Path(os.getenv("BACKUP_ROOT", "/var/backups/reconcilex")).resolve()


def utcnow():
    return datetime.now(timezone.utc)


def run(args, database=None):
    env = dict(os.environ)
    env["PGCONNECT_TIMEOUT"] = "15"
    if database:
        env["PGDATABASE"] = database
    process = subprocess.run(args, env=env, capture_output=True, text=True,
                             timeout=int(os.getenv("PG_COMMAND_TIMEOUT", "3600")))
    if process.returncode:
        # Never include credentials or connection-string arguments in messages.
        raise RuntimeError(f"{args[0]} failed ({process.returncode}): "
                           + process.stderr[-1500:])
    return process.stdout


def s3():
    return boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
    )


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest_path(snapshot):
    if snapshot.is_symlink() or not snapshot.is_dir():
        raise RuntimeError("Not a backup directory")
    if snapshot.parent.resolve() != BACKUP_ROOT or not DIR_PATTERN.fullmatch(snapshot.name):
        raise RuntimeError("Backup must be an immediate dated folder in BACKUP_ROOT")
    return snapshot / "manifest.json"


def snapshots():
    if not BACKUP_ROOT.exists():
        return []
    return sorted(
        (path for path in BACKUP_ROOT.iterdir()
         if path.is_dir() and not path.is_symlink() and
         DIR_PATTERN.fullmatch(path.name) and (path / "manifest.json").is_file()),
        key=lambda path: path.name,
    )


def write_json(path, value):
    with path.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, indent=2, ensure_ascii=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def mkdir_private(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=False)


def backup_once(keep):
    started = time.perf_counter()
    if keep < 1 or keep > 365:
        raise RuntimeError("BACKUP_KEEP must be between 1 and 365")
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    lock = BACKUP_ROOT / ".backup.lock"
    with lock.open("a+b") as guard:
        fcntl.flock(guard.fileno(), fcntl.LOCK_EX)
        name = utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        staging = BACKUP_ROOT / (".pending-" + name)
        final = BACKUP_ROOT / name
        mkdir_private(staging)
        try:
            dump = staging / "database.dump"
            run(["pg_dump", "--format=custom", "--no-owner", "--no-acl",
                 "--file", str(dump)])
            if not dump.is_file() or dump.stat().st_size == 0:
                raise RuntimeError("pg_dump returned an empty archive")
            client = s3()
            bucket = os.environ["S3_BUCKET"]
            entries = []
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not KEY_PATTERN.fullmatch(key):
                        raise RuntimeError("Unexpected object key in source bucket")
                    path = staging / "objects" / key
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with path.open("wb") as stream:
                        response = client.get_object(Bucket=bucket, Key=key)
                        body = response["Body"]
                        try:
                            for chunk in body.iter_chunks(chunk_size=1024 * 1024):
                                if chunk:
                                    stream.write(chunk)
                        finally:
                            body.close()
                        stream.flush()
                        os.fsync(stream.fileno())
                    actual = sha256_file(path)
                    if actual != key.rsplit("/", 1)[-1]:
                        raise RuntimeError("Original object checksum differs from its content address")
                    entries.append({"key": key, "sha256": actual,
                                    "bytes": path.stat().st_size})
            manifest = {
                "schema": 1,
                "created_utc": utcnow().isoformat(),
                "database": {"file": "database.dump", "sha256": sha256_file(dump),
                             "bytes": dump.stat().st_size},
                "bucket": bucket,
                "objects": sorted(entries, key=lambda entry: entry["key"]),
            }
            write_json(staging / "manifest.json", manifest)
            validate_snapshot(staging, require_final=False)
            staging.rename(final)
            # Retention is applied ONLY after complete, verified snapshot publication.
            previous = snapshots()
            for outdated in previous[:-keep]:
                shutil.rmtree(outdated)
            print(json.dumps({"event": "backup_ok", "path": str(final),
                              "object_count": len(entries),
                              "dump_sha256": manifest["database"]["sha256"],
                              "duration_seconds": round(time.perf_counter()-started, 3)}), flush=True)
            return final
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise


def validate_snapshot(path, require_final=True):
    if require_final:
        manifest_path(path)
    elif path.parent.resolve() != BACKUP_ROOT or not path.name.startswith(".pending-"):
        raise RuntimeError("Invalid backup staging directory")
    with (path / "manifest.json").open("r", encoding="utf-8") as stream:
        manifest = json.load(stream)
    if manifest.get("schema") != 1 or manifest["database"]["file"] != "database.dump":
        raise RuntimeError("Unsupported manifest schema")
    dump = path / "database.dump"
    if not dump.is_file() or dump.stat().st_size != manifest["database"]["bytes"]:
        raise RuntimeError("Database dump size mismatch")
    if sha256_file(dump) != manifest["database"]["sha256"]:
        raise RuntimeError("Database dump checksum mismatch")
    keys = set()
    for obj in manifest["objects"]:
        key = obj["key"]
        if not KEY_PATTERN.fullmatch(key) or key in keys:
            raise RuntimeError("Invalid or duplicated key in backup manifest")
        keys.add(key)
        target = (path / "objects" / key).resolve()
        if not target.is_relative_to((path / "objects").resolve()):
            raise RuntimeError("Backup contains unsafe object path")
        if not target.is_file() or target.stat().st_size != obj["bytes"]:
            raise RuntimeError(f"Missing or truncated backup object: {key}")
        digest = sha256_file(target)
        if digest != obj["sha256"] or digest != key.rsplit("/", 1)[-1]:
            raise RuntimeError(f"Checksum mismatch for backup object: {key}")
    print(json.dumps({"event": "backup_verified", "path": str(path),
                      "object_count": len(keys)}), flush=True)
    return manifest


def query(sql, dbname):
    return run(["psql", "--no-psqlrc", "--set", "ON_ERROR_STOP=1",
                "--tuples-only", "--no-align", "--command", sql], database=dbname)


def restore_check(path):
    """Restore into a randomly named temporary DB, NEVER overwrite the live DB."""
    started = time.perf_counter()
    manifest = validate_snapshot(path)
    temporary = "reconcilex_verify_" + uuid.uuid4().hex[:12]
    created = False
    try:
        run(["createdb", "--template=template0", temporary])
        created = True
        run(["pg_restore", "--exit-on-error", "--no-owner", "--no-acl",
             "--dbname", temporary, str(path / "database.dump")])
        rows = query(
            "SELECT json_build_object('key', object_key, 'sha256', file_sha256)::text "
            "FROM import_batches UNION ALL "
            "SELECT json_build_object('key', object_key, 'sha256', file_sha256)::text "
            "FROM import_rejections", temporary,
        )
        objects = {item["key"]: item for item in manifest["objects"]}
        for row in rows.splitlines():
            item = json.loads(row)
            if item["key"] not in objects or objects[item["key"]]["sha256"] != item["sha256"]:
                raise RuntimeError("Database references an original absent from the backup")
        mismatches = query(
            "SELECT count(*) FROM import_batches b LEFT JOIN "
            "(SELECT batch_id,count(*) AS n,coalesce(sum(amount_minor),0) AS total "
            " FROM records GROUP BY batch_id) r ON r.batch_id=b.id "
            "WHERE b.row_count <> coalesce(r.n,0) "
            "OR b.total_minor <> coalesce(r.total,0)", temporary,
        ).strip()
        if mismatches != "0":
            raise RuntimeError(f"Restored financial batches differ: {mismatches}")
        snap_rows = query(
            "SELECT json_build_object('content',content,'sha256',sha256)::text "
            "FROM period_snapshots", temporary,
        )
        for row in snap_rows.splitlines():
            item = json.loads(row)
            canonical = json.dumps(item["content"], sort_keys=True, separators=(",", ":"))
            if hashlib.sha256(canonical.encode("utf-8")).hexdigest() != item["sha256"]:
                raise RuntimeError("Restored closed-period snapshot checksum mismatch")
        print(json.dumps({"event": "restore_verified", "path": str(path),
                          "financial_batches": len(rows.splitlines()),
                          "period_snapshots": len(snap_rows.splitlines()),
                          "duration_seconds": round(time.perf_counter()-started, 3)}), flush=True)
    finally:
        if created:
            run(["dropdb", "--if-exists", "--force", temporary])


def status(max_age_hours):
    """Lightweight service health: a recent completed backup must exist."""
    if max_age_hours <= 0:
        raise RuntimeError("Maximum backup age must be positive")
    found = snapshots()
    if not found:
        raise RuntimeError("No completed backup snapshot available")
    latest = found[-1]
    manifest_path(latest)
    metadata = json.loads((latest / "manifest.json").read_text(encoding="utf-8"))
    created = datetime.fromisoformat(metadata["created_utc"])
    age_hours = (utcnow() - created).total_seconds() / 3600
    if not 0 <= age_hours <= max_age_hours:
        raise RuntimeError(f"Last successful backup is stale ({age_hours:.2f}h)")
    print(json.dumps({"event": "backup_healthy",
                      "age_hours": round(age_hours, 3),
                      "last_snapshot": latest.name}), flush=True)


def schedule():
    keep = int(os.getenv("BACKUP_KEEP", "7"))
    hour = int(os.getenv("BACKUP_HOUR_UTC", "3"))
    if hour < 0 or hour > 23:
        raise RuntimeError("BACKUP_HOUR_UTC must be 0..23")
    while True:
        try:
            backup_once(keep)
        except Exception as exc:
            print(json.dumps({"event": "backup_failed", "message": str(exc)}),
                  file=sys.stderr, flush=True)
            # Keep service alive to retry, do not mark a partial snapshot successful.
        now = utcnow()
        next_run = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        time.sleep(max(1.0, (next_run - now).total_seconds()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once", "verify", "restore-check", "status", "schedule"))
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--keep", type=int, default=int(os.getenv("BACKUP_KEEP", "7")))
    parser.add_argument("--max-age-hours", type=float, default=36)
    args = parser.parse_args()
    try:
        if args.command == "once":
            backup_once(args.keep)
        elif args.command == "schedule":
            schedule()
        elif args.command == "status":
            status(args.max_age_hours)
        else:
            target = args.snapshot.resolve() if args.snapshot else (snapshots()[-1] if snapshots() else None)
            if target is None:
                raise RuntimeError("No completed backups found")
            if args.command == "verify":
                validate_snapshot(target)
            else:
                restore_check(target)
    except Exception as exc:
        print(json.dumps({"event": "backup_error", "message": str(exc)}),
              file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
