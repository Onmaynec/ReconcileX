"""v0.4: security boundaries, token revocation and concurrent financial access.

Uses a disposable seeded PostgreSQL database in CI. No real customer records.
"""
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.api import app
from app.db import SessionLocal
from app.models import (
    AuditEvent, ImportBatch, ImportProfile, MatchGroup, MatchRule, Period,
    ReconcileJob, Source, SourceGrant, User,
)
from app.security import hash_password, token_for
from app.worker import run_once

client = TestClient(app)


@pytest.fixture
def scope():
    suffix = uuid.uuid4().hex[:12]
    secret = "private-test-password-29480"
    with SessionLocal.begin() as db:
        admin = db.scalar(select(User).where(User.username == "admin"))
        assert admin is not None
        src_a = Source(name="security-A-" + suffix, account="ACC-A-" + suffix,
                       currency="RUB")
        src_b = Source(name="security-B-" + suffix, account="ACC-B-" + suffix,
                       currency="RUB")
        period = Period(name="security-" + suffix, start_date=date(2097, 1, 1),
                        end_date=date(2097, 1, 31))
        users = {
            role: User(username="v4-" + role + "-" + suffix,
                       role=role, password_hash=hash_password(secret))
            for role in ("operator", "controller", "auditor", "analyst")
        }
        db.add_all([src_a, src_b, period, *users.values()])
        db.flush()
        db.add_all([
            SourceGrant(source_id=src_a.id, user_id=account.id)
            for account in users.values()
        ] + [SourceGrant(source_id=src_b.id, user_id=users["operator"].id)])
        profile_b = ImportProfile(
            source_id=src_b.id, name="v4-csv", encoding="utf-8",
            delimiter=",", date_format="%Y-%m-%d",
            mapping={"reference": "reference", "date": "date", "amount": "amount"},
        )
        rule_b = MatchRule(
            source_id=src_b.id, rule_key="exact-v4", version=1,
            priority=100, day_window=2, tolerance_minor=0,
            require_reference=True,
        )
        db.add_all([profile_b, rule_b])
        db.flush()
        result = {
            "admin": admin.id,
            "a": src_a.id, "b": src_b.id, "period": period.id,
            "profile_b": profile_b.id,
            "users": {role: account.id for role, account in users.items()},
            "tokens": {"admin": token_for(admin), **{
                role: token_for(account) for role, account in users.items()}},
            "password": secret, "suffix": suffix,
        }
    return result


def auth(scope, role):
    return {"Authorization": "Bearer " + scope["tokens"][role]}


def import_b(scope, kind, ref=None):
    reference = ref or "B-RESTRICTED-" + scope["suffix"]
    # Different byte-level source originals are required for the two kinds;
    # SHA-256 uniqueness is intentionally per-source, not per-kind.
    raw = f"reference,date,amount,origin\n{reference},2097-01-15,10.00,{kind}\n"
    return client.post(
        "/api/v1/imports/commit", headers=auth(scope, "operator"),
        data={"source_id": str(scope["b"]),
              "profile_id": str(scope["profile_b"]),
              "period_id": str(scope["period"]), "kind": kind},
        files={"file": (kind + ".csv", raw.encode(), "text/csv")},
    )


def test_password_change_revokes_existing_jwt(scope):
    operator = scope["users"]["operator"]
    current = auth(scope, "operator")
    result = client.post("/api/v1/auth/change-password", headers=current, json={
        "current_password": scope["password"],
        "new_password": "updated-test-password-482901",
    })
    assert result.status_code == 200, result.text
    assert result.json()["sessions_revoked"]
    assert client.get("/api/v1/sources", headers=current).status_code == 401
    old = client.post("/api/v1/auth/token", json={
        "username": "v4-operator-" + scope["suffix"],
        "password": scope["password"],
    })
    assert old.status_code == 401
    logged_in = client.post("/api/v1/auth/token", json={
        "username": "v4-operator-" + scope["suffix"],
        "password": "updated-test-password-482901",
    })
    assert logged_in.status_code == 200, logged_in.text
    with SessionLocal() as db:
        assert db.get(User, operator).token_epoch == 1
        assert db.scalar(select(func.count(AuditEvent.id)).where(
            AuditEvent.action == "user.password_changed",
            AuditEvent.entity_id == str(operator))) == 1


def test_admin_reset_password_revokes_jwt_without_logging_secret(scope):
    user_id = scope["users"]["analyst"]
    response = client.post(
        f"/api/v1/admin/users/{user_id}/reset-password",
        headers=auth(scope, "admin"),
        json={"new_password": "rotated-secret-for-test-43812"},
    )
    assert response.status_code == 200, response.text
    assert client.get("/api/v1/sources", headers=auth(scope, "analyst")).status_code == 401
    with SessionLocal() as db:
        events = db.scalars(select(AuditEvent).where(
            AuditEvent.action == "user.password_reset",
            AuditEvent.entity_id == str(user_id))).all()
        assert len(events) == 1
        assert "rotated-secret-for-test-43812" not in str(events[0].details)


def test_disabled_account_cannot_use_token_or_login_after_reenable(scope):
    uid = scope["users"]["operator"]
    old_headers = auth(scope, "operator")
    disable = client.post(
        f"/api/v1/admin/users/{uid}/disable", headers=auth(scope, "admin"))
    assert disable.status_code == 200, disable.text
    assert disable.json()["enabled"] is False
    assert client.get("/api/v1/sources", headers=old_headers).status_code == 401
    login = client.post("/api/v1/auth/token", json={
        "username": "v4-operator-" + scope["suffix"],
        "password": scope["password"],
    })
    assert login.status_code == 401
    enable = client.post(
        f"/api/v1/admin/users/{uid}/enable", headers=auth(scope, "admin"))
    assert enable.status_code == 200, enable.text
    assert enable.json()["enabled"] is True
    assert client.get("/api/v1/sources", headers=old_headers).status_code == 401
    login = client.post("/api/v1/auth/token", json={
        "username": "v4-operator-" + scope["suffix"],
        "password": scope["password"],
    })
    assert login.status_code == 200
    assert client.get("/api/v1/sources", headers={
        "Authorization": "Bearer " + login.json()["access_token"]}).status_code == 200


def test_cannot_disable_own_admin(scope):
    response = client.post(
        f"/api/v1/admin/users/{scope['admin']}/disable",
        headers=auth(scope, "admin"))
    assert response.status_code == 409
    with SessionLocal() as db:
        assert db.get(User, scope["admin"]).enabled is True


def test_grant_revocation_immediately_blocks_raw_data(scope):
    assert import_b(scope, "external").status_code == 200
    with SessionLocal() as db:
        batch = db.scalar(select(ImportBatch.id).where(
            ImportBatch.source_id == scope["b"], ImportBatch.period_id == scope["period"]))
        assert batch is not None
    assert client.get(f"/api/v1/imports/{batch}/file",
                      headers=auth(scope, "auditor")).status_code == 403
    with SessionLocal() as db:
        assert db.scalar(select(func.count(AuditEvent.id)).where(
            AuditEvent.action == "security.file_denied",
            AuditEvent.entity_id == str(batch))) >= 1
    assert client.get(f"/api/v1/imports/{batch}/file",
                      headers=auth(scope, "operator")).status_code == 200
    revoke = client.post(
        "/api/v1/admin/grants/revoke", headers=auth(scope, "admin"),
        json={"user_id": str(scope["users"]["operator"]),
              "source_id": str(scope["b"])},
    )
    assert revoke.status_code == 200 and revoke.json()["revoked"] is True
    assert client.get(f"/api/v1/imports/{batch}/file",
                      headers=auth(scope, "operator")).status_code == 403
    assert client.get("/api/v1/records",
                      params={"source_id": str(scope["b"]),
                              "period_id": str(scope["period"])},
                      headers=auth(scope, "operator")).status_code == 403
    assert client.get("/api/v1/exports/records.csv",
                      params={"source_id": str(scope["b"]),
                              "period_id": str(scope["period"])},
                      headers=auth(scope, "operator")).status_code == 403
    with SessionLocal() as db:
        assert db.scalar(select(func.count(AuditEvent.id)).where(
            AuditEvent.action == "source.revoked",
            AuditEvent.source_id == scope["b"])) == 1


def test_worker_rejects_job_after_grant_revocation(scope):
    assert import_b(scope, "external").status_code == 200
    assert import_b(scope, "internal").status_code == 200
    job = client.post(
        "/api/v1/reconciliation/run",
        params={"period_id": str(scope["period"])},
        headers={**auth(scope, "operator"),
                 "Idempotency-Key": "security-grant-revoke-" + scope["suffix"]},
    )
    assert job.status_code == 202, job.text
    revoke = client.post(
        "/api/v1/admin/grants/revoke", headers=auth(scope, "admin"),
        json={"user_id": str(scope["users"]["operator"]),
              "source_id": str(scope["b"])},
    )
    assert revoke.status_code == 200 and revoke.json()["revoked"]
    jid = uuid.UUID(job.json()["id"])
    for _ in range(15):
        with SessionLocal() as db:
            state = db.get(ReconcileJob, jid).state
        if state == "failed":
            break
        assert run_once()
    with SessionLocal() as db:
        saved = db.get(ReconcileJob, jid)
        assert saved.state == "failed", saved.error
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == scope["period"])) == 0


def test_worker_rejects_job_if_submitter_disabled(scope):
    assert import_b(scope, "external").status_code == 200
    job = client.post(
        "/api/v1/reconciliation/run",
        params={"period_id": str(scope["period"])},
        headers={**auth(scope, "operator"),
                 "Idempotency-Key": "security-disable-job-" + scope["suffix"]},
    )
    assert job.status_code == 202, job.text
    disabled = client.post(
        f"/api/v1/admin/users/{scope['users']['operator']}/disable",
        headers=auth(scope, "admin"))
    assert disabled.status_code == 200
    jid = uuid.UUID(job.json()["id"])
    for _ in range(15):
        with SessionLocal() as db:
            state = db.get(ReconcileJob, jid).state
        if state == "failed":
            break
        assert run_once()
    with SessionLocal() as db:
        assert db.get(ReconcileJob, jid).state == "failed"


def test_period_close_and_reopen_enforce_all_source_grants(scope):
    assert import_b(scope, "external").status_code == 200
    # Controller can see source A but not source B (which has the data).
    blocked = client.post(
        f"/api/v1/periods/{scope['period']}/close",
        headers=auth(scope, "controller"))
    assert blocked.status_code == 403, blocked.text
    grant = client.post(
        "/api/v1/admin/grants", headers=auth(scope, "admin"),
        json={"user_id": str(scope["users"]["controller"]),
              "source_id": str(scope["b"])},
    )
    assert grant.status_code == 201, grant.text
    closed = client.post(
        f"/api/v1/periods/{scope['period']}/close",
        headers=auth(scope, "controller"))
    assert closed.status_code == 200, closed.text
    revoke = client.post(
        "/api/v1/admin/grants/revoke", headers=auth(scope, "admin"),
        json={"user_id": str(scope["users"]["controller"]),
              "source_id": str(scope["b"])},
    )
    assert revoke.status_code == 200 and revoke.json()["revoked"]
    rejected = client.post(
        f"/api/v1/periods/{scope['period']}/reopen",
        headers=auth(scope, "controller"),
        json={"reason": "Проверка прав на закрытый финансовый период"},
    )
    assert rejected.status_code == 403, rejected.text
    with SessionLocal() as db:
        assert db.get(Period, scope["period"]).status == "closed"


def test_analyst_cannot_access_raw_rows_or_export(scope):
    assert import_b(scope, "external").status_code == 200
    with SessionLocal() as db:
        batch_id = db.scalar(select(ImportBatch.id).where(
            ImportBatch.period_id == scope["period"]))
    paths = [
        (f"/api/v1/imports/{batch_id}/file", None),
        ("/api/v1/records", {"source_id": str(scope["b"]),
                             "period_id": str(scope["period"])}),
        ("/api/v1/exports/records.csv", {"source_id": str(scope["b"]),
                                         "period_id": str(scope["period"])}),
    ]
    for path, params in paths:
        result = client.get(path, params=params, headers=auth(scope, "analyst"))
        assert result.status_code == 403, (path, result.status_code)


def test_concurrent_period_creation_cannot_overlap(scope):
    # Both requests are allowed to use different names; only one date range
    # can be committed because PostgreSQL serializes overlap checks.
    suffix = scope["suffix"]
    requests = [
        {"name": "concur-a-" + suffix,
         "start_date": "2096-05-01", "end_date": "2096-05-31"},
        {"name": "concur-b-" + suffix,
         "start_date": "2096-05-15", "end_date": "2096-06-15"},
    ]
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(
            client.post, "/api/v1/periods",
            json=body, headers=auth(scope, "admin")) for body in requests]
        statuses = [future.result().status_code for future in futures]
    assert sorted(statuses) == [201, 409], statuses
