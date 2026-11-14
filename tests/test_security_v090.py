"""v0.9 production RBAC acceptance across five roles and source scopes."""
import hashlib
import uuid
from datetime import date

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.api import app
from app.db import SessionLocal
from app.models import (
    AuditEvent, ImportBatch, ImportProfile, ImportRejection, MatchRule,
    Period, PeriodSnapshot, Record, ReconcileIssue, ReconcileJob,
    Source, SourceGrant, User,
)
from app.security import hash_password, token_for

client = TestClient(app)
READ_FINANCIAL = {"operator", "controller", "auditor"}
READ_CONFIGURATION = {"operator", "controller", "admin", "auditor"}
READ_AUDIT = {"controller", "admin", "auditor"}
READ_RAW = {"operator", "controller", "admin", "auditor"}


@pytest.fixture
def role_scope():
    suffix = uuid.uuid4().hex[:10]
    raw_a = f"reference,date,amount\nA-{suffix},2091-01-10,10.00\n".encode()
    raw_b = f"reference,date,amount\nB-{suffix},2091-01-10,20.00\n".encode()
    with SessionLocal.begin() as db:
        admin = db.scalar(select(User).where(User.username == "admin"))
        assert admin is not None
        users = {
            role: User(
                username=f"v090-{role}-{suffix}",
                role=role,
                password_hash=hash_password("v090-test-password-492801"),
            )
            for role in ("operator", "controller", "analyst", "auditor")
        }
        source_a = Source(name=f"v090-A-{suffix}", account=f"ACC-A-{suffix}", currency="RUB")
        source_b = Source(name=f"v090-B-{suffix}", account=f"ACC-B-{suffix}", currency="RUB")
        period = Period(name=f"v090-closed-{suffix}", start_date=date(2091, 1, 1),
                        end_date=date(2091, 1, 31), status="closed")
        open_period = Period(name=f"v090-open-{suffix}", start_date=date(2092, 1, 1),
                             end_date=date(2092, 1, 31))
        db.add_all([*users.values(), source_a, source_b, period, open_period])
        db.flush()
        for account in users.values():
            db.add(SourceGrant(user_id=account.id, source_id=source_a.id))
        profile = ImportProfile(
            source_id=source_a.id, name="v090-profile", encoding="utf-8",
            delimiter=",", date_format="%Y-%m-%d",
            mapping={"reference": "reference", "date": "date", "amount": "amount"},
        )
        rule = MatchRule(
            source_id=source_a.id, rule_key="v090-exact", version=1, priority=100,
            day_window=2, tolerance_minor=0, require_reference=True,
        )
        db.add_all([profile, rule])
        db.flush()

        batch_a = ImportBatch(
            source_id=source_a.id, period_id=period.id, profile_id=profile.id,
            file_sha256=hashlib.sha256(raw_a).hexdigest(),
            object_key=f"originals/{source_a.id}/{hashlib.sha256(raw_a).hexdigest()}",
            filename="a.csv", kind="external", row_count=1, total_minor=1000,
            created_by=users["operator"].id,
        )
        batch_b = ImportBatch(
            source_id=source_b.id, period_id=period.id, profile_id=profile.id,
            file_sha256=hashlib.sha256(raw_b).hexdigest(),
            object_key=f"originals/{source_b.id}/{hashlib.sha256(raw_b).hexdigest()}",
            filename="b.csv", kind="external", row_count=1, total_minor=2000,
            created_by=users["operator"].id,
        )
        db.add_all([batch_a, batch_b])
        db.flush()
        record_a = Record(
            source_id=source_a.id, period_id=period.id, batch_id=batch_a.id,
            kind="external", natural_key=f"na-{suffix}", reference=f"A-{suffix}",
            account=source_a.account, currency="RUB", amount_minor=1000,
            booked_date=date(2091, 1, 10), counterparty=None, row_number=2,
            raw_text=raw_a.decode().splitlines()[1],
            normalized={"reference": f"A-{suffix}", "amount_minor": 1000},
        )
        record_b = Record(
            source_id=source_b.id, period_id=period.id, batch_id=batch_b.id,
            kind="external", natural_key=f"nb-{suffix}", reference=f"B-{suffix}",
            account=source_b.account, currency="RUB", amount_minor=2000,
            booked_date=date(2091, 1, 10), counterparty=None, row_number=2,
            raw_text=raw_b.decode().splitlines()[1],
            normalized={"reference": f"B-{suffix}", "amount_minor": 2000},
        )
        db.add_all([record_a, record_b])
        db.flush()
        issue = ReconcileIssue(
            period_id=period.id, source_id=source_a.id, record_id=record_a.id,
            issue_type="unmatched", candidates=[], state="open",
        )
        open_issue = ReconcileIssue(
            period_id=open_period.id, source_id=source_a.id, record_id=None,
            issue_type="unmatched", candidates=[], state="open",
        )
        rejection = ImportRejection(
            source_id=source_a.id, period_id=period.id, profile_id=profile.id,
            file_sha256=hashlib.sha256(b"rejected").hexdigest(),
            object_key=f"rejected/{source_a.id}/{hashlib.sha256(b'rejected').hexdigest()}",
            filename="rejected.csv", errors=[{"line": 2, "type": "invalid"}],
            created_by=users["operator"].id,
        )
        job = ReconcileJob(
            period_id=period.id, idempotency_key=f"v090-job-{suffix}",
            rules_snapshot=[{
                "id": str(rule.id), "key": rule.rule_key, "version": 1,
                "priority": 100, "window": 2, "tolerance": 0,
                "require_reference": True, "source_id": str(source_a.id),
            }],
            state="failed", created_by=users["operator"].id,
        )
        report = {
            "period_id": str(period.id), "period_name": period.name,
            "by_source": [
                {"source_id": str(source_a.id), "account": source_a.account,
                 "currency": "RUB", "external_count": 1, "external_minor": 1000,
                 "internal_count": 0, "internal_minor": 0, "confirmed_external_minor": 0,
                 "confirmed_count": 0, "open_issues": 1, "difference_minor": 1000},
                {"source_id": str(source_b.id), "account": source_b.account,
                 "currency": "RUB", "external_count": 1, "external_minor": 2000,
                 "internal_count": 0, "internal_minor": 0, "confirmed_external_minor": 0,
                 "confirmed_count": 0, "open_issues": 0, "difference_minor": 2000},
            ],
        }
        period.snapshot = {"schema_version": 1, "report": report}
        period.snapshot_sha256 = "a" * 64
        db.add_all([issue, open_issue, rejection, job])
        db.flush()
        db.add(PeriodSnapshot(
            period_id=period.id, sequence=1, content={"report": report},
            sha256="a" * 64, closed_by=admin.id,
        ))
        db.add(AuditEvent(
            source_id=source_a.id, actor_id=users["operator"].id,
            action="test.seeded", entity_type="Source", entity_id=str(source_a.id),
            details={"fixture": "v0.9.0"},
        ))
        result = {
            "suffix": suffix, "admin": admin.id,
            "users": {role: account.id for role, account in users.items()},
            "tokens": {"admin": token_for(admin), **{
                role: token_for(account) for role, account in users.items()}},
            "source_a": source_a.id, "source_b": source_b.id,
            "period": period.id, "open_period": open_period.id,
            "profile": profile.id, "rule": rule.id, "batch_a": batch_a.id,
            "batch_b": batch_b.id, "record_a": record_a.id, "issue": issue.id,
            "open_issue": open_issue.id, "rejection": rejection.id, "job": job.id,
            "raw_a": raw_a, "raw_b": raw_b, "raw_rejection": b"rejected",
        }
    return result


def auth(scope, role):
    return {"Authorization": "Bearer " + scope["tokens"][role]}


def test_read_routes_enforce_role_matrix_and_source_grants(role_scope):
    scope = role_scope
    params = {"source_id": str(scope["source_a"]), "period_id": str(scope["period"])}
    for role in ("operator", "controller", "analyst", "auditor", "admin"):
        sources = client.get("/api/v1/sources", headers=auth(scope, role))
        assert sources.status_code == 200
        visible = {item["id"] for item in sources.json()}
        expected = {str(scope["source_a"]), str(scope["source_b"])} if role == "admin" else {str(scope["source_a"])}
        assert visible == expected

        report = client.get(f"/api/v1/reports/period/{scope['period']}",
                            headers=auth(scope, role))
        assert report.status_code == 200
        report_sources = {item["source_id"] for item in report.json()["by_source"]}
        assert report_sources == expected

        for endpoint in ("/api/v1/records", "/api/v1/issues",
                         "/api/v1/exports/records.csv"):
            response = client.get(endpoint, params=params, headers=auth(scope, role))
            assert response.status_code == (200 if role in READ_FINANCIAL else 403), (
                role, endpoint, response.status_code, response.text)

        for endpoint in ("/api/v1/profiles", "/api/v1/rules"):
            response = client.get(endpoint, params={"source_id": str(scope["source_a"])},
                                  headers=auth(scope, role))
            assert response.status_code == (200 if role in READ_CONFIGURATION else 403), (
                role, endpoint, response.status_code, response.text)

        audit = client.get("/api/v1/audit", params={"source_id": str(scope["source_a"])},
                           headers=auth(scope, role))
        assert audit.status_code == (200 if role in READ_AUDIT else 403)

        snapshots = client.get(f"/api/v1/periods/{scope['period']}/snapshots",
                               headers=auth(scope, role))
        assert snapshots.status_code == (200 if role in READ_AUDIT else 403)
        if role in {"controller", "auditor"}:
            assert {row["source_id"] for row in snapshots.json()[0]["report"]["by_source"]} == {
                str(scope["source_a"])}

        metrics = client.get("/api/v1/admin/metrics", headers=auth(scope, role))
        assert metrics.status_code == (200 if role == "admin" else 403)


def test_admin_mutations_are_restricted_to_admin(role_scope):
    scope = role_scope
    uid = str(scope["users"]["operator"])
    source = str(scope["source_a"])
    calls = [
        ("POST", "/api/v1/sources", {"name": "v090-denied-source", "account": "DENIED", "currency": "RUB"}),
        ("POST", "/api/v1/admin/users", {"username": "v090-denied-user", "password": "long-test-password-123", "role": "operator"}),
        ("POST", f"/api/v1/admin/users/{uid}/reset-password", {"new_password": "another-long-test-password-123"}),
        ("POST", f"/api/v1/admin/users/{uid}/disable", None),
        ("POST", f"/api/v1/admin/users/{uid}/enable", None),
        ("POST", "/api/v1/admin/grants", {"user_id": uid, "source_id": source}),
        ("POST", "/api/v1/admin/grants/revoke", {"user_id": uid, "source_id": source}),
        ("POST", "/api/v1/profiles", {
            "source_id": source, "name": "denied-profile",
            "mapping": {"reference": "ref", "date": "date", "amount": "amount"},
        }),
        ("POST", "/api/v1/periods", {
            "name": "v090-denied-period", "start_date": "2093-01-01", "end_date": "2093-01-31",
        }),
        ("POST", "/api/v1/rules", {
            "source_id": source, "rule_key": "denied-rule", "priority": 1,
            "day_window": 1, "tolerance_minor": 0, "require_reference": True,
        }),
        ("GET", "/api/v1/admin/users", None),
        ("GET", f"/api/v1/admin/grants?user_id={uid}", None),
    ]
    for role in ("operator", "controller", "analyst", "auditor"):
        for method, path, body in calls:
            response = client.request(method, path, headers=auth(scope, role), json=body)
            assert response.status_code == 403, (role, method, path, response.status_code, response.text)


def test_operator_controller_and_submitter_permissions(role_scope):
    scope = role_scope
    preview = client.post(
        "/api/v1/imports/preview",
        headers=auth(scope, "operator"),
        data={"source_id": str(scope["source_a"]), "profile_id": str(scope["profile"]),
              "period_id": str(scope["period"]), "kind": "external"},
        files={"file": ("x.csv", b"reference,date,amount\nX,2091-01-10,1.00\n", "text/csv")},
    )
    assert preview.status_code == 409  # operator passes RBAC; period is intentionally closed
    for role in ("controller", "analyst", "auditor", "admin"):
        denied = client.post(
            "/api/v1/imports/preview", headers=auth(scope, role),
            data={"source_id": str(scope["source_a"]), "profile_id": str(scope["profile"]),
                  "period_id": str(scope["period"]), "kind": "external"},
            files={"file": ("x.csv", b"reference,date,amount\nX,2091-01-10,1.00\n", "text/csv")},
        )
        assert denied.status_code == 403

    for role in ("operator", "controller"):
        response = client.post(
            "/api/v1/reconciliation/run", params={"period_id": str(scope["period"])},
            headers={**auth(scope, role), "Idempotency-Key": f"v090-run-{role}-key"},
        )
        assert response.status_code == 409  # role passed; the test period is closed
    for role in ("analyst", "auditor", "admin"):
        response = client.post(
            "/api/v1/reconciliation/run", params={"period_id": str(scope["period"])},
            headers={**auth(scope, role), "Idempotency-Key": f"v090-deny-{role}-key"},
        )
        assert response.status_code == 403

    body = {
        "period_id": str(scope["period"]), "source_id": str(scope["source_a"]),
        "rule_id": str(scope["rule"]), "external_ids": [str(uuid.uuid4())],
        "internal_ids": [str(uuid.uuid4())], "comment": "manual role matrix check",
    }
    response = client.post("/api/v1/match-groups", json=body,
                           headers={**auth(scope, "operator"), "Idempotency-Key": "v090-manual-key"})
    assert response.status_code == 409
    for role in ("controller", "analyst", "auditor", "admin"):
        response = client.post("/api/v1/match-groups", json=body,
                               headers={**auth(scope, role), "Idempotency-Key": f"v090-deny-{role}"})
        assert response.status_code == 403

    missing_group = uuid.uuid4()
    for role in ("operator", "analyst", "auditor", "admin"):
        response = client.post(f"/api/v1/match-groups/{missing_group}/approve",
                               json={"expected_version": 1}, headers=auth(scope, role))
        assert response.status_code == 403
    assert client.post(f"/api/v1/match-groups/{missing_group}/approve",
                       json={"expected_version": 1}, headers=auth(scope, "controller")).status_code == 404
    for role in ("operator", "analyst", "auditor", "admin"):
        response = client.post(f"/api/v1/match-groups/{missing_group}/cancel",
                               json={"expected_version": 1, "reason": "a sufficiently long cancellation reason"},
                               headers=auth(scope, role))
        assert response.status_code == 403
    assert client.post(f"/api/v1/match-groups/{missing_group}/cancel",
                       json={"expected_version": 1, "reason": "a sufficiently long cancellation reason"},
                       headers=auth(scope, "controller")).status_code == 404

    job = str(scope["job"])
    for role in ("operator", "admin"):
        assert client.get(f"/api/v1/reconciliation/jobs/{job}",
                          headers=auth(scope, role)).status_code == 200
    for role in ("controller", "analyst", "auditor"):
        assert client.get(f"/api/v1/reconciliation/jobs/{job}",
                          headers=auth(scope, role)).status_code == 403
    assert client.post(f"/api/v1/reconciliation/jobs/{job}/retry",
                       headers=auth(scope, "operator")).status_code == 409
    for role in ("controller", "analyst", "auditor", "admin"):
        assert client.post(f"/api/v1/reconciliation/jobs/{job}/retry",
                           headers=auth(scope, role)).status_code == 403

    close = client.post(f"/api/v1/periods/{scope['open_period']}/close",
                        headers=auth(scope, "controller"))
    assert close.status_code == 200, close.text
    assert client.post(f"/api/v1/periods/{scope['open_period']}/reopen",
                       json={"reason": "approved reopening for RBAC acceptance"},
                       headers=auth(scope, "controller")).status_code == 200
    for role in ("operator", "analyst", "auditor", "admin"):
        assert client.post(f"/api/v1/periods/{scope['open_period']}/close",
                           headers=auth(scope, role)).status_code == 403

    assign = client.post(
        f"/api/v1/issues/{scope['open_issue']}/assign",
        json={"owner_id": str(scope["users"]["operator"])},
        headers=auth(scope, "operator"),
    )
    assert assign.status_code == 200, assign.text
    assign_again = client.post(
        f"/api/v1/issues/{scope['open_issue']}/assign",
        json={"owner_id": str(scope["users"]["operator"])},
        headers=auth(scope, "controller"),
    )
    assert assign_again.status_code == 200, assign_again.text
    for role in ("analyst", "auditor", "admin"):
        assert client.post(
            f"/api/v1/issues/{scope['open_issue']}/assign",
            json={"owner_id": str(scope["users"]["operator"])},
            headers=auth(scope, role),
        ).status_code == 403

    resolved = client.post(
        f"/api/v1/issues/{scope['open_issue']}/resolve",
        json={"reason": "resolved during production RBAC acceptance"},
        headers=auth(scope, "operator"),
    )
    assert resolved.status_code == 200
    again = client.post(
        f"/api/v1/issues/{scope['open_issue']}/resolve",
        json={"reason": "resolved during production RBAC acceptance"},
        headers=auth(scope, "controller"),
    )
    assert again.status_code == 409
    for role in ("analyst", "auditor", "admin"):
        assert client.post(
            f"/api/v1/issues/{scope['open_issue']}/resolve",
            json={"reason": "resolved during production RBAC acceptance"},
            headers=auth(scope, role),
        ).status_code == 403


def test_original_file_denials_are_audited_and_raw_file_roles_are_consistent(role_scope, monkeypatch):
    scope = role_scope
    import app.api as api_module
    monkeypatch.setattr(api_module, "get_original", lambda key: (
        scope["raw_a"] if key == f"originals/{scope['source_a']}/{hashlib.sha256(scope['raw_a']).hexdigest()}"
        else scope["raw_rejection"]
    ))
    for role in ("operator", "controller", "admin", "auditor"):
        response = client.get(f"/api/v1/imports/{scope['batch_a']}/file",
                              headers=auth(scope, role))
        assert response.status_code == 200
        rejected = client.get(f"/api/v1/imports/rejections/{scope['rejection']}/file",
                              headers=auth(scope, role))
        assert rejected.status_code == 200
    assert client.get(f"/api/v1/imports/{scope['batch_a']}/file",
                      headers=auth(scope, "analyst")).status_code == 403
    assert client.get(f"/api/v1/imports/rejections/{scope['rejection']}/file",
                      headers=auth(scope, "analyst")).status_code == 403

    for role in ("operator", "controller", "analyst", "auditor"):
        response = client.get(f"/api/v1/imports/{scope['batch_b']}/file",
                              headers=auth(scope, role))
        assert response.status_code == 403
    with SessionLocal() as db:
        denied = db.scalars(select(AuditEvent).where(
            AuditEvent.action == "security.file_denied",
            AuditEvent.entity_id.in_([str(scope["batch_a"]), str(scope["batch_b"]),
                                      str(scope["rejection"])])
        )).all()
        assert len(denied) >= 6
        assert all(event.created_at is not None for event in denied)
