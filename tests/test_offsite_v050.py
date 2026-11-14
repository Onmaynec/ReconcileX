"""v0.5 off-site backup regressions: no network or customer records."""
import hashlib
import io
import json
import os
import uuid
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from ops import backup, offsite


class FakePaginator:
    def __init__(self, storage):
        self.storage = storage

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [
            {"Key": key} for key in sorted(self.storage.objects)
            if key.startswith(Prefix)
        ]}


class FakeRemote:
    def __init__(self):
        self.objects = {}
        self.upload_count = 0
        self.fail_after_write = False

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "not found"}},
                              "HeadObject")
        body, metadata, encryption, kms = self.objects[Key]
        result = {
            "ContentLength": len(body), "Metadata": metadata,
            "ServerSideEncryption": encryption,
        }
        if kms is not None:
            result["SSEKMSKeyId"] = kms
        return result

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs):
        if Key in self.objects:
            raise AssertionError("Existing off-site objects must not be overwritten")
        self.upload_count += 1
        with open(Filename, "rb") as source:
            self.objects[Key] = (
                source.read(), ExtraArgs["Metadata"],
                ExtraArgs["ServerSideEncryption"], ExtraArgs.get("SSEKMSKeyId"),
            )
        if self.fail_after_write:
            self.fail_after_write = False
            raise OSError("simulated network loss after object upload")

    def put_object(self, Bucket, Key, Body, IfNoneMatch, Metadata, **kwargs):
        if Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        assert IfNoneMatch == "*"
        self.objects[Key] = (
            Body, Metadata, kwargs["ServerSideEncryption"],
            kwargs.get("SSEKMSKeyId"),
        )

    def get_paginator(self, command):
        assert command == "list_objects_v2"
        return FakePaginator(self)


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(backup, "BACKUP_ROOT", tmp_path.resolve())
    for name, value in {
        "OFFSITE_ENABLED": "1",
        "OFFSITE_S3_ENDPOINT": "https://archive.example.invalid",
        "OFFSITE_S3_BUCKET": "isolated-finance-backups",
        "OFFSITE_S3_ACCESS_KEY": "independent-access",
        "OFFSITE_S3_SECRET_KEY": "independent-secret",
        "OFFSITE_MANIFEST_HMAC_KEY": "test-hmac-key-" + "A" * 55,
        "OFFSITE_S3_PREFIX": "reconcilex",
        "OFFSITE_S3_REGION": "us-east-1",
        "OFFSITE_S3_ENCRYPTION": "AES256",
        "S3_ENDPOINT": "http://silo:9000",
        "S3_BUCKET": "reconcilex",
        "S3_ACCESS_KEY": "application-access",
        "S3_SECRET_KEY": "application-secret",
    }.items():
        monkeypatch.setenv(name, value)
    fake = FakeRemote()
    monkeypatch.setattr(offsite, "client_for", lambda config: fake)
    return tmp_path, fake, offsite.settings()


def sample_snapshot(root, name=None):
    name = name or ("20261114T081000Z-" + uuid.uuid4().hex[:8])
    snapshot = root / name
    snapshot.mkdir(mode=0o700)
    dump = snapshot / "database.dump"
    dump.write_bytes(b"synthetic PG dump for recovery tests")
    payload = b"synthetic CSV original (no customer data)"
    digest = hashlib.sha256(payload).hexdigest()
    key = "originals/00000000-0000-0000-0000-000000000000/" + digest
    original = snapshot / "objects" / key
    original.parent.mkdir(parents=True)
    original.write_bytes(payload)
    manifest = {
        "schema": 1, "created_utc": datetime.now(timezone.utc).isoformat(),
        "database": {
            "file": "database.dump", "bytes": dump.stat().st_size,
            "sha256": hashlib.sha256(dump.read_bytes()).hexdigest(),
        },
        "bucket": "reconcilex",
        "objects": [{"key": key, "bytes": len(payload), "sha256": digest}],
    }
    (snapshot / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8",
    )
    return snapshot, manifest


def test_encrypted_hmac_signed_remote_replication_and_download(isolated):
    root, fake, conf = isolated
    snapshot, manifest = sample_snapshot(root)
    assert backup.validate_snapshot(snapshot) == manifest
    assert offsite.upload_snapshot(snapshot, manifest, client=fake, config=conf)
    assert len(fake.objects) == 3
    assert all(value[2] == "AES256" for value in fake.objects.values())
    verified = offsite.verify_snapshot(snapshot.name, expected=manifest,
                                       client=fake, config=conf)
    assert verified == manifest
    assert offsite.latest_snapshot_name(client=fake, config=conf) == snapshot.name
    destination = root / "downloaded"
    assert offsite.download_snapshot(snapshot.name, destination,
                                     client=fake, config=conf) == manifest
    assert (destination / "database.dump").read_bytes() == (snapshot / "database.dump").read_bytes()
    assert backup.sha256_file(destination / "database.dump") == manifest["database"]["sha256"]
    with pytest.raises(FileExistsError):
        offsite.download_snapshot(snapshot.name, destination, client=fake, config=conf)
    assert not offsite.upload_snapshot(snapshot, manifest, client=fake, config=conf)
    assert fake.upload_count == 2


def test_retry_after_partial_network_failure_never_overwrites_object(isolated):
    root, fake, conf = isolated
    snapshot, manifest = sample_snapshot(root)
    fake.fail_after_write = True
    with pytest.raises(OSError, match="network loss"):
        offsite.upload_snapshot(snapshot, manifest, client=fake, config=conf)
    assert len(fake.objects) == 1
    assert not any(key.endswith("manifest.json") for key in fake.objects)
    assert offsite.upload_snapshot(snapshot, manifest, client=fake, config=conf)
    assert fake.upload_count == 2
    assert offsite.verify_snapshot(snapshot.name, expected=manifest,
                                   client=fake, config=conf) == manifest


def test_reject_remote_manifest_tampering_even_with_rewritten_sha(isolated):
    root, fake, conf = isolated
    snapshot, manifest = sample_snapshot(root)
    offsite.upload_snapshot(snapshot, manifest, client=fake, config=conf)
    key = offsite.key_for(conf, snapshot.name, "manifest.json")
    raw, metadata, encryption, kms = fake.objects[key]
    changed = raw.replace(b"reconcilex", b"somethinge")
    metadata = dict(metadata, sha256=hashlib.sha256(changed).hexdigest())
    fake.objects[key] = changed, metadata, encryption, kms
    with pytest.raises(RuntimeError, match="authentication"):
        offsite.verify_snapshot(snapshot.name, client=fake, config=conf, full=False)


def test_reject_corrupted_remote_object_and_unencrypted_copy(isolated):
    root, fake, conf = isolated
    snapshot, manifest = sample_snapshot(root)
    offsite.upload_snapshot(snapshot, manifest, client=fake, config=conf)
    key = offsite.key_for(conf, snapshot.name, "database.dump")
    raw, metadata, enc, kms = fake.objects[key]
    fake.objects[key] = b"Z" + raw[1:], metadata, enc, kms
    with pytest.raises(RuntimeError, match="checksum"):
        offsite.verify_snapshot(snapshot.name, client=fake, config=conf)
    fake.objects[key] = raw, metadata, None, kms
    with pytest.raises(RuntimeError, match="encryption"):
        offsite.verify_snapshot(snapshot.name, client=fake, config=conf)


@pytest.mark.parametrize("bad_name", ["../other", "a/b", "20260101Z", ""])
def test_remote_key_path_traversal_is_rejected(isolated, bad_name):
    _, _, conf = isolated
    with pytest.raises(RuntimeError, match="snapshot name"):
        offsite.key_for(conf, bad_name, "manifest.json")


def test_reject_invalid_manifest_object_key_and_duplicate(isolated):
    root, _, _ = isolated
    _, manifest = sample_snapshot(root)
    manifest["objects"][0]["key"] = "../../secrets.txt"
    with pytest.raises(RuntimeError, match="Unsafe"):
        offsite.validate_manifest(manifest)


def test_configuration_requires_https_distinct_storage_and_signing_key(isolated, monkeypatch):
    _, _, _ = isolated
    monkeypatch.setenv("OFFSITE_S3_ENDPOINT", "http://archive.example.invalid")
    with pytest.raises(RuntimeError, match="HTTPS"):
        offsite.settings()
    monkeypatch.setenv("OFFSITE_S3_ENDPOINT", "https://archive.example.invalid")
    monkeypatch.setenv("OFFSITE_S3_ACCESS_KEY", "application-access")
    with pytest.raises(RuntimeError, match="separate credentials"):
        offsite.settings()
    monkeypatch.setenv("OFFSITE_S3_ACCESS_KEY", "independent-access")
    monkeypatch.setenv("OFFSITE_MANIFEST_HMAC_KEY", "too-short")
    with pytest.raises(RuntimeError, match="at least 32"):
        offsite.settings()


def test_offsite_status_fails_closed_if_latest_remote_missing(isolated):
    root, fake, conf = isolated
    snapshot, manifest = sample_snapshot(root)
    offsite.upload_snapshot(snapshot, manifest, client=fake, config=conf)
    backup.status(36)
    newer, _ = sample_snapshot(root, "20261114T082000Z-a1b2c3d4")
    assert newer.name != snapshot.name
    with pytest.raises(RuntimeError, match="no completed manifest"):
        backup.status(36)


def test_offsite_drill_uses_random_temporary_folder_and_cleans_up(isolated, monkeypatch):
    root, fake, conf = isolated
    snapshot, manifest = sample_snapshot(root)
    offsite.upload_snapshot(snapshot, manifest, client=fake, config=conf)
    observed = []
    def checked_restore(path):
        assert path != snapshot
        assert backup.validate_snapshot(path) == manifest
        observed.append(path)
    monkeypatch.setattr(backup, "restore_check", checked_restore)
    backup.drill_offsite(snapshot.name)
    assert len(observed) == 1
    assert not observed[0].exists()


def test_offsite_failure_prevents_local_retention(isolated, monkeypatch):
    root, _, _ = isolated
    old, _ = sample_snapshot(root, "20261114T000000Z-a1b2c3d4")

    def pretend_dump(args, database=None):
        assert args[0] == "pg_dump"
        (root / PathHack.path_in_args(args)).write_bytes(b"test database dump")
        return ""

    class PathHack:
        @staticmethod
        def path_in_args(args):
            # pg_dump --file always points at the newly created staging file.
            path = args[args.index("--file") + 1]
            return os.path.relpath(path, root)

    class EmptyS3:
        def get_paginator(self, action):
            assert action == "list_objects_v2"
            class Empty:
                def paginate(self, Bucket):
                    yield {}
            return Empty()

    monkeypatch.setattr(backup, "run", pretend_dump)
    monkeypatch.setattr(backup, "s3", lambda: EmptyS3())
    monkeypatch.setattr(backup, "replicate_all", lambda: (_ for _ in ()).throw(
        RuntimeError("offsite unavailable")))
    with pytest.raises(RuntimeError, match="offsite unavailable"):
        backup.backup_once(1)
    assert old.exists()
    assert len(backup.snapshots()) == 2
