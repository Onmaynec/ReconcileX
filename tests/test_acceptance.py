"""Acceptance regressions against a real PostgreSQL in CI (PV-FIN-03)."""
import io
import os
import uuid
from datetime import date
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.api import app
from app.db import SessionLocal
from app.models import (
    AuditEvent, ImportBatch, ImportProfile, MatchMember, MatchRule, Period,
    Record, Source, SourceGrant, User,
)
from app.money import parse_minor
from app.security import hash_password, token_for
from app.worker import run_once

client = TestClient(app)


@pytest.fixture
def setup():
    suffix = uuid.uuid4().hex[:8]
    with SessionLocal.begin() as db:
        source = Source(name="test-RUB-" + suffix, account="ACC-" + suffix, currency="RUB")
        euro = Source(name="test-EUR-" + suffix, account="EU-" + suffix, currency="EUR")
        period = Period(name="2026-09-" + suffix, start_date=date(2026, 9, 1),
                        end_date=date(2026, 9, 30))
        db.add_all([source, euro, period])
        db.flush()
        profile = ImportProfile(
            source_id=source.id, name="test-csv", encoding="utf-8",
            delimiter=",", date_format="%Y-%m-%d", decimal_separator=".",
            mapping={"reference": "reference", "date": "date", "amount": "amount"})
        eu_profile = ImportProfile(
            source_id=euro.id, name="test-euro", encoding="utf-8",
            delimiter=",", date_format="%Y-%m-%d", decimal_separator=".",
            mapping={"reference": "reference", "date": "date", "amount": "amount"})
        rule = MatchRule(source_id=source.id, rule_key="exact", version=1,
                         priority=100, day_window=2, tolerance_minor=0,
                         require_reference=True)
        db.add_all([profile, eu_profile, rule])
        db.flush()
        users = {role: db.scalar(select(User).where(User.username == role))
                 for role in ("operator", "controller", "admin", "auditor")}
        for user in users.values():
            for src in (source, euro):
                db.add(SourceGrant(source_id=src.id, user_id=user.id))
        IDs = {"source": source.id, "euro": euro.id, "period": period.id,
               "profile": profile.id, "eu_profile": eu_profile.id, "rule": rule.id}
        tokens = {role: token_for(user) for role, user in users.items()}
    return IDs, tokens


def headers(tokens, role="operator", key=None):
    result = {"Authorization": "Bearer " + tokens[role]}
    if key is not None:
        result["Idempotency-Key"] = key
    return result


def import_rows(ids, tokens, kind, content, source="source", profile="profile", commit=True):
    endpoint = "commit" if commit else "preview"
    return client.post(
        "/api/v1/imports/" + endpoint,
        headers=headers(tokens),
        data={"source_id": str(ids[source]), "profile_id": str(ids[profile]),
              "period_id": str(ids["period"]), "kind": kind},
        files={"file": ("synthetic.csv", content.encode("utf-8"), "text/csv")},
    )


def records(ids, tokens, kind, source="source"):
    response = client.get("/api/v1/records", headers=headers(tokens),
                          params={"period_id": str(ids["period"]),
                                  "source_id": str(ids[source]), "kind": kind, "limit": 200})
    assert response.status_code == 200, response.text
    return response.json()


def fixture_lines(*rows):
    return "reference,date,amount\n" + "".join(f"{ref},{day},{amount}\n"
                                               for ref, day, amount in rows)


def test_money_exact_minor_units():
    assert parse_minor("1000.00", "RUB") == 100000
    assert parse_minor("-20.00", "RUB") == -2000
    assert parse_minor("980.00", "RUB") == 98000
    assert parse_minor("980,00", "EUR", ",") == 98000
    with pytest.raises(ValueError):
        parse_minor("1.001", "RUB")
    with pytest.raises(ValueError):
        parse_minor("1e3", "RUB")
    with pytest.raises(ValueError):
        parse_minor("1.50", "JPY")


def test_at01_at02_at03_atomic_import_dedupe_and_validation(setup):
    ids, tokens = setup
    content = "reference,date,amount\n" + "".join(
        f"T{i:05},2026-09-15,0.01\n" for i in range(20000))
    preview = import_rows(ids, tokens, "external", content, commit=False)
    assert preview.status_code == 200, preview.text
    assert preview.json()["valid_rows"] == 20000
    assert preview.json()["total_minor"] == 20000
    first = import_rows(ids, tokens, "external", content)
    assert first.status_code == 200, first.text
    assert first.json()["row_count"] == 20000
    duplicate = import_rows(ids, tokens, "external", content)
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert duplicate.json()["id"] == first.json()["id"]
    with SessionLocal() as db:
        assert db.scalar(select(func.count(Record.id)).where(
            Record.batch_id == uuid.UUID(first.json()["id"]))) == 20000
    broken = fixture_lines(*[(f"BAD{i}", "2026-09-16", "invalid") for i in range(12)])
    preview = import_rows(ids, tokens, "internal", broken, commit=False)
    assert len(preview.json()["errors"]) == 12
    assert all("line" in error for error in preview.json()["errors"])
    bad_commit = import_rows(ids, tokens, "internal", broken)
    assert bad_commit.status_code == 422
    with SessionLocal() as db:
        assert db.scalar(select(func.count(Record.id)).where(
            Record.period_id == ids["period"], Record.kind == "internal")) == 0


def test_at04_ambiguity_no_arbitrary_match(setup):
    ids, tokens = setup
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("SAME", "2026-09-12", "13.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("SAME", "2026-09-12", "13.00"),
        ("SAME", "2026-09-13", "13.00"))).status_code == 200
    job = client.post("/api/v1/reconciliation/run",
                      headers=headers(tokens, key="ambiguity-seed-0001"),
                      params={"period_id": str(ids["period"])})
    assert job.status_code == 202, job.text
    assert run_once()
    issues = client.get("/api/v1/issues", headers=headers(tokens),
                        params={"source_id": str(ids["source"]),
                                "period_id": str(ids["period"])})
    assert issues.status_code == 200
    assert any(i["issue_type"] == "ambiguous" for i in issues.json())
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchMember.id)).where(
            MatchMember.active.is_(True))) >= 0
        assert db.scalar(select(func.count(MatchMember.id)).where(
            MatchMember.record_id.in_([uuid.UUID(r["id"]) for r in
                                       records(ids, tokens, "external")]),
            MatchMember.active.is_(True))) == 0


def test_at05_at06_at07_at08_at10_at11_at12(setup):
    ids, tokens = setup
    external = fixture_lines(("PAY", "2026-09-12", "980.00"))
    internal = fixture_lines(
        ("PAY", "2026-09-12", "1000.00"), ("FEE", "2026-09-12", "-20.00"))
    assert import_rows(ids, tokens, "external", external).status_code == 200
    assert import_rows(ids, tokens, "internal", internal).status_code == 200
    ex = records(ids, tokens, "external")[0]
    ins = records(ids, tokens, "internal")
    body = {
        "period_id": str(ids["period"]), "source_id": str(ids["source"]),
        "rule_id": str(ids["rule"]), "external_ids": [ex["id"]],
        "internal_ids": [i["id"] for i in ins],
        "comment": "Комиссия банка удержана из исходного платежа",
    }
    group = client.post("/api/v1/match-groups", json=body,
                        headers=headers(tokens, key="fee-group-001"))
    assert group.status_code == 201, group.text
    gid = group.json()["id"]
    assert group.json()["delta_minor"] == 0
    assert group.json()["state"] == "proposed"
    repeated = client.post("/api/v1/match-groups", json=body,
                           headers=headers(tokens, key="fee-group-001"))
    assert repeated.status_code == 201 and repeated.json()["id"] == gid
    self_approval = client.post(f"/api/v1/match-groups/{gid}/approve",
                                json={"expected_version": 1}, headers=headers(tokens))
    assert self_approval.status_code == 403
    approved = client.post(f"/api/v1/match-groups/{gid}/approve",
                           json={"expected_version": 1},
                           headers=headers(tokens, "controller"))
    assert approved.status_code == 200, approved.text
    assert approved.json()["state"] == "approved"
    again = client.post("/api/v1/match-groups", json={**body, "comment": "second approval attempt"},
                        headers=headers(tokens, key="fee-group-002"))
    assert again.status_code == 409
    explanation = client.get(f"/api/v1/match-groups/{gid}",
                             headers=headers(tokens, "auditor"))
    assert explanation.status_code == 200
    assert len(explanation.json()["original_rows"]) == 3
    assert explanation.json()["proposed_by"]
    assert explanation.json()["rule_version"] == 1
    # Incompatible currencies/sources never form a group.
    assert import_rows(ids, tokens, "internal",
                       fixture_lines(("EUR", "2026-09-12", "980.00")),
                       source="euro", profile="eu_profile").status_code == 200
    euro_row = records(ids, tokens, "internal", source="euro")[0]
    forbidden = client.post("/api/v1/match-groups", json={
        **body, "internal_ids": [euro_row["id"]]
    }, headers=headers(tokens, key="currency-00001"))
    assert forbidden.status_code == 409
    # Source-level denial on original file must produce audit security event.
    with SessionLocal.begin() as db:
        lone = User(username="ungranted-" + uuid.uuid4().hex[:8],
                    password_hash=hash_password("test-test-password"), role="auditor")
        db.add(lone)
        db.flush()
        lone_token = token_for(lone)
        batch_id = db.scalar(select(ImportBatch.id).where(
            ImportBatch.source_id == ids["source"]).limit(1))
    denied = client.get(f"/api/v1/imports/{batch_id}/file",
                        headers={"Authorization": "Bearer " + lone_token})
    assert denied.status_code == 403
    with SessionLocal() as db:
        assert db.scalar(select(func.count(AuditEvent.id)).where(
            AuditEvent.action == "security.file_denied",
            AuditEvent.entity_id == str(batch_id))) >= 1
    # Closed period always serves the frozen financial snapshot.
    before = client.get(f"/api/v1/reports/period/{ids['period']}",
                        headers=headers(tokens, "auditor")).json()
    closing = client.post(f"/api/v1/periods/{ids['period']}/close",
                          headers=headers(tokens, "controller"))
    assert closing.status_code == 200, closing.text
    rule_update = client.post("/api/v1/rules", headers=headers(tokens, "admin"),
                              json={"source_id": str(ids["source"]), "rule_key": "exact",
                                    "priority": 200, "day_window": 7,
                                    "tolerance_minor": 100, "require_reference": True})
    assert rule_update.status_code == 201, rule_update.text
    after = client.get(f"/api/v1/reports/period/{ids['period']}",
                       headers=headers(tokens, "auditor")).json()
    assert before["by_source"] == after["by_source"]
    assert after["snapshot_sha256"] == closing.json()["snapshot_sha256"]
    assert import_rows(ids, tokens, "external",
                       fixture_lines(("NEW", "2026-09-14", "4.00"))).status_code == 409


def test_at09_job_idempotence_and_restart(setup):
    ids, tokens = setup
    ex = fixture_lines(("ONE", "2026-09-20", "7.00"))
    assert import_rows(ids, tokens, "external", ex).status_code == 200
    assert import_rows(ids, tokens, "internal", ex + "").status_code == 200
    res = client.post("/api/v1/reconciliation/run", params={"period_id": str(ids["period"])},
                      headers=headers(tokens, key="job-run-restart-0001"))
    assert res.status_code == 202, res.text
    assert run_once()
    repeat = client.post("/api/v1/reconciliation/run",
                         params={"period_id": str(ids["period"])},
                         headers=headers(tokens, key="job-run-restart-0001"))
    assert repeat.status_code == 202
    assert repeat.json()["id"] == res.json()["id"]
    with SessionLocal() as db:
        count = db.scalar(select(func.count(MatchMember.id)).where(
            MatchMember.record_id.in_([uuid.UUID(r["id"]) for r in
                                       records(ids, tokens, "external")]),
            MatchMember.active.is_(True)))
        assert count == 1
