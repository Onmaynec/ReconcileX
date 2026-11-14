"""Negative recovery tests for v0.2.0, using synthetic data only."""
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.api import app
from app.db import SessionLocal
from app.models import ImportProfile, Period, Record, Source, User
from app.security import token_for
from ops import backup

client = TestClient(app, raise_server_exceptions=False)


def test_backup_manifest_rejects_tampered_dump_and_object(monkeypatch, tmp_path):
    monkeypatch.setattr(backup, "BACKUP_ROOT", tmp_path.resolve())
    snapshot = tmp_path / "20261114T050000Z-deadbeef"
    snapshot.mkdir()
    dump = snapshot / "database.dump"
    dump.write_bytes(b"synthetic PostgreSQL archive")
    payload = b"synthetic original file"
    digest = hashlib.sha256(payload).hexdigest()
    key = "originals/00000000-0000-0000-0000-000000000000/" + digest
    target = snapshot / "objects" / key
    target.parent.mkdir(parents=True)
    target.write_bytes(payload)
    manifest = {
        "schema": 1,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "database": {
            "file": "database.dump", "sha256": hashlib.sha256(dump.read_bytes()).hexdigest(),
            "bytes": dump.stat().st_size,
        },
        "bucket": "reconcilex",
        "objects": [{"key": key, "sha256": digest, "bytes": len(payload)}],
    }
    (snapshot / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert backup.validate_snapshot(snapshot)["objects"][0]["sha256"] == digest
    target.write_bytes(b"modified")
    with pytest.raises(RuntimeError, match="truncated"):
        backup.validate_snapshot(snapshot)
    target.write_bytes(payload)
    dump.write_bytes(b"X" + dump.read_bytes()[1:])
    with pytest.raises(RuntimeError, match="checksum"):
        backup.validate_snapshot(snapshot)


def test_backup_health_detects_stale_copy(monkeypatch, tmp_path):
    monkeypatch.setattr(backup, "BACKUP_ROOT", tmp_path.resolve())
    with pytest.raises(RuntimeError, match="No completed"):
        backup.status(36)


def test_s3_outage_does_not_commit_financial_records(monkeypatch):
    source_name = "demo-rouble"
    with SessionLocal() as db:
        source = db.scalar(select(Source).where(Source.name == source_name))
        period = db.scalar(select(Period).where(Period.name == "2026-08"))
        profile = db.scalar(select(ImportProfile).where(
            ImportProfile.source_id == source.id,
            ImportProfile.name == "demo-csv").limit(1))
        operator = db.scalar(select(User).where(User.username == "operator"))
        baseline = db.scalar(select(func.count(Record.id)).where(
            Record.source_id == source.id, Record.period_id == period.id))
        source_id, profile_id, period_id = source.id, profile.id, period.id
        authorization = {"Authorization": "Bearer " + token_for(operator)}

    def raise_storage_error(key, data):
        raise ConnectionError("injected S3 storage outage")

    monkeypatch.setattr("app.api.put_original", raise_storage_error)
    reference = "S3-OUTAGE-" + uuid.uuid4().hex
    contents = f"reference,date,amount\n{reference},2026-08-18,12.40\n".encode()
    response = client.post(
        "/api/v1/imports/commit",
        headers=authorization,
        data={"source_id": str(source_id), "profile_id": str(profile_id),
              "period_id": str(period_id), "kind": "external"},
        files={"file": ("outage.csv", contents, "text/csv")},
    )
    assert response.status_code == 500, response.text
    with SessionLocal() as db:
        after = db.scalar(select(func.count(Record.id)).where(
            Record.source_id == source_id, Record.period_id == period_id))
    assert after == baseline
