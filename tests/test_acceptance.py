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
    assert import_rows(ids, tokens, "internal", fixture_lines(("ONE", "2026-09-21", "7.00"))).status_code == 200
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


def test_at06_parallel_confirmation_rejects_second_claim(setup):
    ids, tokens = setup
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("CONCURRENT", "2026-09-12", "2000.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("CONCURRENT", "2026-09-12", "2000.00"),
        ("CONCURRENT", "2026-09-13", "2000.00"))).status_code == 200
    external = records(ids, tokens, "external")[0]
    internal = records(ids, tokens, "internal")
    groups = []
    for idx, row in enumerate(internal):
        response = client.post("/api/v1/match-groups", headers=headers(
            tokens, key=f"parallel-approval-{idx:04}"), json={
            "period_id": str(ids["period"]), "source_id": str(ids["source"]),
            "rule_id": str(ids["rule"]), "external_ids": [external["id"]],
            "internal_ids": [row["id"]], "comment": "Контроль конкурирующей сверки",
        })
        assert response.status_code == 201, response.text
        assert response.json()["state"] == "proposed"
        groups.append(response.json()["id"])

    def approve(group_id):
        return client.post(f"/api/v1/match-groups/{group_id}/approve",
                           json={"expected_version": 1},
                           headers=headers(tokens, "controller")).status_code

    with ThreadPoolExecutor(max_workers=2) as executor:
        states = list(executor.map(approve, groups))
    assert sorted(states) == [200, 409]


def test_generated_six_formats_and_invalid_fixtures(tmp_path):
    import json
    import subprocess
    import sys
    from types import SimpleNamespace
    from app.importer import inspect_file

    subprocess.run([sys.executable, "scripts/generate_demo.py", "--rows", "120",
                    "--out", str(tmp_path)], check=True)
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["rows"] == 120
    assert len(manifest["files"]) == 6
    for spec in manifest["files"]:
        source = SimpleNamespace(currency=spec["currency"], account="FIXTURE-ACCOUNT")
        profile = SimpleNamespace(
            encoding=spec["encoding"], delimiter=spec["delimiter"],
            mapping=spec["mapping"], date_format=spec["date_format"],
            decimal_separator=spec["decimal_separator"])
        result = inspect_file(
            (tmp_path / spec["file"]).read_bytes(), spec["file"],
            profile, source, spec["kind"],
            date(2026, spec["month"], 1), date(2026, spec["month"], 28))
        assert result["valid_rows"] == 20, spec["file"]
        assert not result["errors"], (spec["file"], result["errors"])
    rub = manifest["files"][0]
    source = SimpleNamespace(currency="RUB", account="FIXTURE-ACCOUNT")
    profile = SimpleNamespace(
        encoding="utf-8", delimiter=",",
        mapping={"reference": "reference", "date": "date", "amount": "amount"},
        date_format="%Y-%m-%d", decimal_separator=".")
    invalid = inspect_file(
        (tmp_path / "invalid_rows.csv").read_bytes(), "invalid_rows.csv",
        profile, source, "external", date(2026, 8, 1), date(2026, 8, 31))
    assert len(invalid["errors"]) == 12
    duplicate = inspect_file(
        (tmp_path / "duplicate_rows.csv").read_bytes(), "duplicate_rows.csv",
        profile, source, "external", date(2026, 8, 1), date(2026, 8, 31))
    assert len(duplicate["errors"]) == 1


def test_rate_limit_returns_429_for_repeated_invalid_logins():
    username = "unknown-" + uuid.uuid4().hex
    for _ in range(5):
        result = client.post("/api/v1/auth/token",
                             json={"username": username, "password": "bad"})
        assert result.status_code == 401, result.text
    response = client.post("/api/v1/auth/token",
                           json={"username": username, "password": "bad"})
    assert response.status_code == 429
    assert response.json()["code"] == "429"


def test_closed_snapshots_survive_reopen_and_reclose(setup):
    ids, tokens = setup
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("FIRST", "2026-09-10", "10.00"))).status_code == 200
    first = client.post(f"/api/v1/periods/{ids['period']}/close",
                        headers=headers(tokens, "controller"))
    assert first.status_code == 200, first.text
    original_hash = first.json()["snapshot_sha256"]
    reopened = client.post(f"/api/v1/periods/{ids['period']}/reopen",
                           json={"reason": "Исправление финансового реестра месяца"},
                           headers=headers(tokens, "controller"))
    assert reopened.status_code == 200, reopened.text
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("SECOND", "2026-09-11", "20.00"))).status_code == 200
    second = client.post(f"/api/v1/periods/{ids['period']}/close",
                         headers=headers(tokens, "controller"))
    assert second.status_code == 200, second.text
    assert second.json()["snapshot_sha256"] != original_hash
    history = client.get(f"/api/v1/periods/{ids['period']}/snapshots",
                         headers=headers(tokens, "auditor"))
    assert history.status_code == 200, history.text
    versions = history.json()
    assert len(versions) == 2
    assert versions[0]["snapshot_sha256"] == original_hash
    assert versions[0]["report"]["by_source"][0]["external_minor"] == 1000
    assert versions[1]["report"]["by_source"][0]["external_minor"] == 3000


def test_worker_crash_rolls_back_and_retry_succeeds(setup, monkeypatch):
    import app.worker as worker
    from app.models import ReconcileJob

    ids, tokens = setup
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("CRASH", "2026-09-20", "9.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("CRASH", "2026-09-21", "9.00"))).status_code == 200
    job_resp = client.post("/api/v1/reconciliation/run",
                           headers=headers(tokens, key="crash-injection-job-0001"),
                           params={"period_id": str(ids["period"])})
    assert job_resp.status_code == 202, job_resp.text
    job_id = job_resp.json()["id"]
    original = worker.reconcile

    def crash_after_flushing(db, job):
        original(db, job)
        db.flush()
        raise RuntimeError("injected failure after transactional match")

    monkeypatch.setattr(worker, "reconcile", crash_after_flushing)
    assert worker.run_once()
    with SessionLocal() as db:
        job = db.get(ReconcileJob, uuid.UUID(job_id))
        assert job.state == "failed"
        assert not db.scalar(select(MatchMember.id).where(
            MatchMember.record_id.in_([uuid.UUID(r["id"]) for r in
                                       records(ids, tokens, "external")]),
            MatchMember.active.is_(True)))
    retry = client.post(f"/api/v1/reconciliation/jobs/{job_id}/retry",
                        headers=headers(tokens))
    assert retry.status_code == 200, retry.text
    monkeypatch.setattr(worker, "reconcile", original)
    assert worker.run_once()
    with SessionLocal() as db:
        job = db.get(ReconcileJob, uuid.UUID(job_id))
        assert job.state == "done"
        assert db.scalar(select(func.count(MatchMember.id)).where(
            MatchMember.record_id.in_([uuid.UUID(r["id"]) for r in
                                       records(ids, tokens, "external")]),
            MatchMember.active.is_(True))) == 1


def test_same_priority_rules_with_different_candidates_are_ambiguous(setup):
    ids, tokens = setup
    new_rule = client.post("/api/v1/rules", headers=headers(tokens, "admin"), json={
        "source_id": str(ids["source"]), "rule_key": "same-priority-broad",
        "priority": 100, "day_window": 2,
        "tolerance_minor": 0, "require_reference": False,
    })
    assert new_rule.status_code == 201, new_rule.text
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("SPECIFIC", "2026-09-15", "20.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("SPECIFIC", "2026-09-15", "20.00"),
        ("DIFFERENT", "2026-09-15", "20.00"))).status_code == 200
    submitted = client.post("/api/v1/reconciliation/run",
                            headers=headers(tokens, key="same-priority-rules-0001"),
                            params={"period_id": str(ids["period"])})
    assert submitted.status_code == 202, submitted.text
    assert run_once()
    issues_result = client.get("/api/v1/issues", headers=headers(tokens),
                               params={"source_id": str(ids["source"]),
                                       "period_id": str(ids["period"])})
    assert issues_result.status_code == 200, issues_result.text
    assert any(issue["issue_type"] == "ambiguous" and len(issue["candidates"]) == 2
               for issue in issues_result.json())
    with SessionLocal() as db:
        assert not db.scalar(select(MatchMember.id).where(
            MatchMember.record_id.in_([
                uuid.UUID(row["id"]) for row in records(ids, tokens, "external")
            ]), MatchMember.active.is_(True)))


def test_wrong_currency_and_duplicates_create_auditable_exceptions(setup):
    ids, tokens = setup
    profile = client.post("/api/v1/profiles", headers=headers(tokens, "admin"), json={
        "source_id": str(ids["source"]), "name": "currency-field",
        "mapping": {"reference": "reference", "date": "date",
                    "amount": "amount", "currency": "currency"},
    })
    assert profile.status_code == 201, profile.text
    ids["currency_profile"] = uuid.UUID(profile.json()["id"])
    bad_file = "reference,date,amount,currency\nBAD-CUR,2026-09-14,10.00,EUR\n"
    preview = import_rows(ids, tokens, "external", bad_file,
                          profile="currency_profile", commit=False)
    assert preview.status_code == 200
    assert preview.json()["errors"][0]["type"] == "wrong_currency"
    rejected = import_rows(ids, tokens, "external", bad_file,
                           profile="currency_profile")
    assert rejected.status_code == 422, rejected.text
    rejection_id = rejected.headers["X-Rejection-ID"]
    original = client.get(f"/api/v1/imports/rejections/{rejection_id}/file",
                          headers=headers(tokens, "auditor"))
    assert original.status_code == 200 and original.content == bad_file.encode("utf-8")
    first = import_rows(ids, tokens, "external", fixture_lines(
        ("ORIGINAL", "2026-09-15", "12.00")))
    assert first.status_code == 200, first.text
    duplicate = import_rows(ids, tokens, "external", fixture_lines(
        ("ORIGINAL", "2026-09-15", "12.00"),
        ("NEW-RECORD", "2026-09-15", "10.00")))
    assert duplicate.status_code == 409, duplicate.text
    assert duplicate.headers["X-Rejection-ID"]
    with SessionLocal() as db:
        assert db.scalar(select(func.count(Record.id)).where(
            Record.period_id == ids["period"], Record.source_id == ids["source"])) == 1
    issues_resp = client.get("/api/v1/issues", headers=headers(tokens),
                             params={"source_id": str(ids["source"]),
                                     "period_id": str(ids["period"])})
    assert issues_resp.status_code == 200
    issue_types = {i["issue_type"] for i in issues_resp.json()}
    assert {"wrong_currency", "duplicate"} <= issue_types
    duplicate_issue = next(i for i in issues_resp.json() if i["issue_type"] == "duplicate")
    with SessionLocal() as db:
        operator_id = db.scalar(select(User.id).where(User.username == "operator"))
    claimed = client.post(f"/api/v1/issues/{duplicate_issue['id']}/assign",
                          json={"owner_id": str(operator_id)}, headers=headers(tokens))
    assert claimed.status_code == 200, claimed.text
    resolved = client.post(f"/api/v1/issues/{duplicate_issue['id']}/resolve",
                           json={"reason": "Исходная запись уже имеется в реестре"},
                           headers=headers(tokens))
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["state"] == "resolved"


def test_at07_self_approval_denied_even_after_role_change(setup):
    from app.models import SourceGrant
    from app.security import hash_password, token_for

    ids, tokens = setup
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("SELF", "2026-09-16", "2200.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("SELF", "2026-09-17", "2200.00"))).status_code == 200
    external_id = records(ids, tokens, "external")[0]["id"]
    internal_id = records(ids, tokens, "internal")[0]["id"]

    with SessionLocal.begin() as db:
        author = User(
            username="role-change-" + uuid.uuid4().hex[:10],
            role="operator", password_hash=hash_password("synthetic-password-123"))
        db.add(author)
        db.flush()
        db.add(SourceGrant(source_id=ids["source"], user_id=author.id))
        author_id = author.id
        author_token = token_for(author)
    proposed = client.post("/api/v1/match-groups",
                           headers={"Authorization": "Bearer " + author_token,
                                    "Idempotency-Key": "self-approval-check-0001"},
                           json={"period_id": str(ids["period"]),
                                 "source_id": str(ids["source"]),
                                 "rule_id": str(ids["rule"]),
                                 "external_ids": [external_id],
                                 "internal_ids": [internal_id],
                                 "comment": "Проверка самосогласования после смены роли"})
    assert proposed.status_code == 201, proposed.text
    assert proposed.json()["state"] == "proposed"
    with SessionLocal.begin() as db:
        account = db.get(User, author_id)
        account.role = "controller"
    refused = client.post(f"/api/v1/match-groups/{proposed.json()['id']}/approve",
                          headers={"Authorization": "Bearer " + author_token},
                          json={"expected_version": 1})
    assert refused.status_code == 403, refused.text
    assert "Self-approval" in refused.json()["message"]


def test_period_close_rejects_incomplete_jobs(setup):
    ids, tokens = setup
    submitted = client.post("/api/v1/reconciliation/run",
                            headers=headers(tokens, key="unsettled-close-check-01"),
                            params={"period_id": str(ids["period"])})
    assert submitted.status_code == 202
    refused = client.post(f"/api/v1/periods/{ids['period']}/close",
                          headers=headers(tokens, "controller"))
    assert refused.status_code == 409
    assert run_once()
    accepted = client.post(f"/api/v1/periods/{ids['period']}/close",
                           headers=headers(tokens, "controller"))
    assert accepted.status_code == 200, accepted.text


def test_role_access_limits_raw_rows_exports_and_issues(setup):
    ids, tokens = setup
    analyst = client.post("/api/v1/auth/token", json={
        "username": "analyst", "password": os.environ["DEMO_PASSWORD"]})
    assert analyst.status_code == 200, analyst.text
    auth = {"Authorization": "Bearer " + analyst.json()["access_token"]}
    data_params = {"source_id": str(ids["source"]), "period_id": str(ids["period"])}
    assert client.get("/api/v1/records", params=data_params,
                      headers=auth).status_code == 403
    assert client.get("/api/v1/issues", params=data_params,
                      headers=auth).status_code == 403
    assert client.get("/api/v1/exports/records.csv", params=data_params,
                      headers=auth).status_code == 403
    assert client.get("/api/v1/reports/period/" + str(ids["period"]),
                      headers=auth).status_code == 200


def test_operator_cannot_read_ungranted_source(setup):
    from app.models import SourceGrant
    from app.security import token_for, hash_password

    ids, tokens = setup
    with SessionLocal.begin() as db:
        limited = User(username="limited-" + uuid.uuid4().hex[:12],
                       password_hash=hash_password("not-for-production-123"),
                       role="operator")
        db.add(limited)
        db.flush()
        db.add(SourceGrant(user_id=limited.id, source_id=ids["source"]))
        limited_token = token_for(limited)
    auth = {"Authorization": "Bearer " + limited_token}
    result = client.get("/api/v1/records", params={
        "source_id": str(ids["euro"]), "period_id": str(ids["period"])
    }, headers=auth)
    assert result.status_code == 403, result.text
    valid = client.get("/api/v1/records", params={
        "source_id": str(ids["source"]), "period_id": str(ids["period"])
    }, headers=auth)
    assert valid.status_code == 200, valid.text


def test_indexed_reconciliation_bulk_pairs_and_repeat_are_idempotent(setup):
    """Fast single-rule path must preserve every original financial invariant."""
    from app.models import MatchGroup, ReconcileIssue
    ids, tokens = setup
    pairs = 620  # exercise two full write batches and a final partial one
    rows = [(f"FAST-{i:05d}", f"2026-09-{(i % 28) + 1:02d}", "10.00")
            for i in range(pairs)]
    assert import_rows(ids, tokens, "external", fixture_lines(*rows)).status_code == 200
    internal_file = "reference,date,amount,origin\n" + "".join(
        f"{ref},{day},{amount},internal\n" for ref, day, amount in rows)
    assert import_rows(ids, tokens, "internal", internal_file).status_code == 200
    job = client.post(
        "/api/v1/reconciliation/run",
        headers=headers(tokens, key="indexed-bulk-one-00001"),
        params={"period_id": str(ids["period"])},
    )
    assert job.status_code == 202, job.text
    assert run_once()
    with SessionLocal() as db:
        groups = db.scalars(select(MatchGroup).where(
            MatchGroup.period_id == ids["period"],
            MatchGroup.source_id == ids["source"])).all()
        assert len(groups) == pairs
        assert all(g.state == "approved" and g.delta_minor == 0 and
                   g.rule_version == 1 for g in groups)
        active_members = db.scalar(select(func.count(MatchMember.id)).join(
            MatchGroup, MatchGroup.id == MatchMember.group_id).where(
                MatchGroup.period_id == ids["period"],
                MatchMember.active.is_(True)))
        assert active_members == pairs * 2
        assert db.scalar(select(func.count(AuditEvent.id)).where(
            AuditEvent.source_id == ids["source"],
            AuditEvent.action == "match.confirmed")) == pairs
        assert db.scalar(select(func.count(ReconcileIssue.id)).where(
            ReconcileIssue.period_id == ids["period"])) == 0
    repeat = client.post(
        "/api/v1/reconciliation/run",
        headers=headers(tokens, key="indexed-bulk-two-00002"),
        params={"period_id": str(ids["period"])},
    )
    assert repeat.status_code == 202, repeat.text
    assert run_once()
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == pairs
    report = client.get(
        f"/api/v1/reports/period/{ids['period']}",
        headers=headers(tokens, "controller"))
    assert report.status_code == 200
    summaries = [item for item in report.json()["by_source"]
                 if item["source_id"] == str(ids["source"])]
    assert len(summaries) == 1
    assert summaries[0]["confirmed_count"] == pairs
    assert summaries[0]["external_minor"] == pairs * 1000
    assert summaries[0]["difference_minor"] == 0


def test_indexed_rule_cannot_guess_reverse_ambiguous_pair(setup):
    from app.models import MatchGroup
    ids, tokens = setup
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("REF-REVERSE", "2026-09-15", "10.00"),
        ("REF-REVERSE", "2026-09-16", "10.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("REF-REVERSE", "2026-09-15", "10.00"))).status_code == 200
    submitted = client.post(
        "/api/v1/reconciliation/run",
        headers=headers(tokens, key="indexed-reverse-ambiguity-01"),
        params={"period_id": str(ids["period"])},
    )
    assert submitted.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == 0
    issues = client.get("/api/v1/issues", headers=headers(tokens),
                        params={"source_id": str(ids["source"]),
                                "period_id": str(ids["period"])})
    assert issues.status_code == 200
    ambiguous = [i for i in issues.json() if i["issue_type"] == "ambiguous"]
    assert len(ambiguous) == 2
    assert all(len(i["candidates"]) == 2 for i in ambiguous)


def test_indexed_rule_high_risk_keeps_four_eyes_proposal(setup):
    from app.models import MatchGroup
    ids, tokens = setup
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("HIGH-INDEXED", "2026-09-15", "1500.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal",
                       "reference,date,amount,origin\nHIGH-INDEXED,2026-09-15,1500.00,internal\n").status_code == 200
    submitted = client.post(
        "/api/v1/reconciliation/run",
        headers=headers(tokens, key="indexed-four-eyes-0001"),
        params={"period_id": str(ids["period"])},
    )
    assert submitted.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        group = db.scalar(select(MatchGroup).where(
            MatchGroup.period_id == ids["period"]))
        assert group and group.state == "proposed" and group.version == 1
        group_id = group.id
        assert db.scalar(select(func.count(MatchMember.id)).where(
            MatchMember.group_id == group_id, MatchMember.active.is_(True))) == 0
    denied = client.post(
        f"/api/v1/match-groups/{group_id}/approve",
        json={"expected_version": 1}, headers=headers(tokens))
    assert denied.status_code == 403
    accepted = client.post(
        f"/api/v1/match-groups/{group_id}/approve",
        json={"expected_version": 1}, headers=headers(tokens, "controller"))
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["state"] == "approved"



def _set_tolerant_rule(ids, cents=5):
    """Update the rule before queuing a job, preserving the test source."""
    with SessionLocal.begin() as db:
        rule = db.get(MatchRule, ids["rule"])
        rule.tolerance_minor = cents


def test_tolerant_indexed_matching_keeps_exact_minor_unit_deltas(setup):
    from app.models import MatchGroup, ReconcileIssue
    ids, tokens = setup
    _set_tolerant_rule(ids, 5)
    # 620 pairs cross the 500-write boundary. Amounts deliberately differ.
    pairs = 620
    ext = fixture_lines(*[
        (f"TOL-{i:05d}", f"2026-09-{(i % 28) + 1:02d}", "10.00")
        for i in range(pairs)])
    internal = "reference,date,amount,origin\n" + "".join(
        f"TOL-{i:05d},2026-09-{(i % 28) + 1:02d},"
        f"{'9.97' if i % 2 == 0 else '10.02'},internal\n"
        for i in range(pairs))
    assert import_rows(ids, tokens, "external", ext).status_code == 200
    assert import_rows(ids, tokens, "internal", internal).status_code == 200
    job = client.post("/api/v1/reconciliation/run",
                      headers=headers(tokens, key="tolerance-bulk-v060-001"),
                      params={"period_id": str(ids["period"])})
    assert job.status_code == 202, job.text
    assert run_once()
    with SessionLocal() as db:
        groups = db.scalars(select(MatchGroup).where(
            MatchGroup.period_id == ids["period"],
            MatchGroup.source_id == ids["source"])).all()
        assert len(groups) == pairs
        assert all(g.state == "approved" for g in groups)
        assert sorted([g.delta_minor for g in groups]) == [(-2)] * (pairs // 2) + [3] * (pairs // 2)
        assert all(g.explanation["tolerance_minor"] == 5 for g in groups)
        assert db.scalar(select(func.count(MatchMember.id)).join(
            MatchGroup, MatchMember.group_id == MatchGroup.id).where(
            MatchGroup.period_id == ids["period"],
            MatchMember.active.is_(True))) == 2 * pairs
        assert not db.scalar(select(ReconcileIssue.id).where(
            ReconcileIssue.period_id == ids["period"]))
        logs = db.scalars(select(AuditEvent).where(
            AuditEvent.source_id == ids["source"],
            AuditEvent.action == "match.confirmed")).all()
        assert len(logs) == pairs
        assert sum(x.details["delta_minor"] for x in logs) == pairs // 2
    repeat = client.post("/api/v1/reconciliation/run",
                         headers=headers(tokens, key="tolerance-bulk-v060-002"),
                         params={"period_id": str(ids["period"])})
    assert repeat.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == pairs


def test_tolerant_indexed_rule_never_guesses_forward_or_reverse_ambiguity(setup):
    from app.models import MatchGroup, ReconcileIssue
    ids, tokens = setup
    _set_tolerant_rule(ids, 5)
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("FWD", "2026-09-15", "10.00"),
        ("REV", "2026-09-16", "20.00"),
        ("REV", "2026-09-17", "20.02"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("FWD", "2026-09-15", "9.98"),
        ("FWD", "2026-09-15", "10.03"),
        ("REV", "2026-09-16", "20.01"))).status_code == 200
    job = client.post("/api/v1/reconciliation/run",
                      headers=headers(tokens, key="tolerance-ambig-v060-001"),
                      params={"period_id": str(ids["period"])})
    assert job.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == 0
        ambiguous = db.scalars(select(ReconcileIssue).where(
            ReconcileIssue.period_id == ids["period"],
            ReconcileIssue.issue_type == "ambiguous")).all()
        assert len(ambiguous) == 3
        assert all(len(i.candidates) >= 2 for i in ambiguous)


def test_tolerant_indexed_rule_does_not_match_outside_amount_or_date_window(setup):
    from app.models import MatchGroup, ReconcileIssue
    ids, tokens = setup
    _set_tolerant_rule(ids, 5)
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("OVER", "2026-09-15", "10.00"),
        ("DATE", "2026-09-15", "20.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("OVER", "2026-09-15", "9.94"),
        ("DATE", "2026-09-18", "20.03"))).status_code == 200
    job = client.post("/api/v1/reconciliation/run",
                      headers=headers(tokens, key="tolerance-bounds-v060-001"),
                      params={"period_id": str(ids["period"])})
    assert job.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == 0
        assert db.scalar(select(func.count(ReconcileIssue.id)).where(
            ReconcileIssue.period_id == ids["period"])) == 2


def test_tolerant_indexed_rule_high_risk_still_requires_controller(setup):
    from app.models import MatchGroup
    ids, tokens = setup
    _set_tolerant_rule(ids, 5)
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("HIGH-TOL", "2026-09-15", "1500.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("HIGH-TOL", "2026-09-15", "1499.99"))).status_code == 200
    job = client.post("/api/v1/reconciliation/run",
                      headers=headers(tokens, key="tolerance-four-eyes-v060-001"),
                      params={"period_id": str(ids["period"])})
    assert job.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        group = db.scalar(select(MatchGroup).where(
            MatchGroup.period_id == ids["period"]))
        assert group.state == "proposed" and group.delta_minor == 1
        assert group.explanation["tolerance_minor"] == 5
        group_id = group.id
    deny = client.post(f"/api/v1/match-groups/{group_id}/approve",
                       headers=headers(tokens),
                       json={"expected_version": 1})
    assert deny.status_code == 403
    yes = client.post(f"/api/v1/match-groups/{group_id}/approve",
                      headers=headers(tokens, role="controller"),
                      json={"expected_version": 1})
    assert yes.status_code == 200 and yes.json()["state"] == "approved"



def test_v070_multiple_priorities_bulk_are_indexed_and_financially_exact(setup):
    """Three rule levels, 621 pairs and no SQL lookup for each individual row."""
    from collections import Counter
    from sqlalchemy import event
    from app.db import engine
    from app.models import MatchGroup, ReconcileIssue

    ids, tokens = setup
    for name, priority, tolerance, require_ref in (
        ("tolerant-fallback", 90, 5, True),
        ("broad-fallback", 80, 0, False),
    ):
        response = client.post("/api/v1/rules",
                               headers=headers(tokens, "admin"), json={
            "source_id": str(ids["source"]), "rule_key": name,
            "priority": priority, "day_window": 2,
            "tolerance_minor": tolerance, "require_reference": require_ref,
        })
        assert response.status_code == 201, response.text

    rows_external, rows_internal = [], []
    for i in range(620):
        day = f"2026-09-{(i % 28) + 1:02d}"
        ref = f"MULTI-{i:05d}"
        rows_external.append((ref, day, "10.00"))
        rows_internal.append((ref, day, "10.00" if i < 500 else "9.97"))
    rows_external.append(("BROAD-EXTERNAL", "2026-09-20", "88.21"))
    rows_internal.append(("BROAD-INTERNAL", "2026-09-20", "88.21"))
    assert import_rows(ids, tokens, "external", fixture_lines(*rows_external)).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(*rows_internal)).status_code == 200

    submitted = client.post(
        "/api/v1/reconciliation/run",
        headers=headers(tokens, key="multi-bulk-priority-0070001"),
        params={"period_id": str(ids["period"])},
    )
    assert submitted.status_code == 202, submitted.text
    record_queries = []

    def count_record_queries(conn, cursor, statement, params, context, executemany):
        query = statement.lstrip().lower()
        if query.startswith("select") and "from records" in query and " between " in query:
            record_queries.append(statement)

    event.listen(engine, "before_cursor_execute", count_record_queries)
    try:
        assert run_once()
    finally:
        event.remove(engine, "before_cursor_execute", count_record_queries)
    assert not record_queries, record_queries[:2]

    with SessionLocal() as db:
        groups = db.scalars(select(MatchGroup).where(
            MatchGroup.period_id == ids["period"],
            MatchGroup.source_id == ids["source"])).all()
        assert len(groups) == 621
        assert Counter(g.explanation["rule_key"] for g in groups) == {
            "exact": 500, "tolerant-fallback": 120, "broad-fallback": 1,
        }
        assert sum(group.delta_minor for group in groups) == 360
        assert all(group.state == "approved" for group in groups)
        assert db.scalar(select(func.count(MatchMember.id)).join(
            MatchGroup, MatchMember.group_id == MatchGroup.id).where(
                MatchGroup.period_id == ids["period"], MatchMember.active.is_(True),
        )) == 1242
        assert db.scalar(select(func.count(ReconcileIssue.id)).where(
            ReconcileIssue.period_id == ids["period"])) == 0
        audit_rows = db.scalars(select(AuditEvent).where(
            AuditEvent.source_id == ids["source"],
            AuditEvent.action == "match.confirmed")).all()
        assert len(audit_rows) == 621
        assert sum(event.details["delta_minor"] for event in audit_rows) == 360

    repeat = client.post(
        "/api/v1/reconciliation/run",
        headers=headers(tokens, key="multi-bulk-priority-0070002"),
        params={"period_id": str(ids["period"])},
    )
    assert repeat.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == 621


def test_v070_same_priority_multiple_rules_one_candidate_is_not_ambiguous(setup):
    from app.models import MatchGroup
    ids, tokens = setup
    response = client.post("/api/v1/rules",
                           headers=headers(tokens, "admin"), json={
        "source_id": str(ids["source"]), "rule_key": "z-broad",
        "priority": 100, "day_window": 2, "tolerance_minor": 0,
        "require_reference": False,
    })
    assert response.status_code == 201, response.text
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("IDENTICAL", "2026-09-12", "18.01"))).status_code == 200
    assert import_rows(ids, tokens, "internal",
                       "reference,date,amount,origin\n"
                       "IDENTICAL,2026-09-12,18.01,internal\n").status_code == 200
    job = client.post("/api/v1/reconciliation/run",
                      headers=headers(tokens, key="multi-same-one-00700001"),
                      params={"period_id": str(ids["period"])})
    assert job.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        group = db.scalar(select(MatchGroup).where(
            MatchGroup.period_id == ids["period"]))
        assert group is not None and group.state == "approved"
        assert group.explanation["rule_key"] == "exact"
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == 1


def test_v070_reverse_ambiguity_across_equal_priority_rules_blocks_match(setup):
    from app.models import MatchGroup, ReconcileIssue
    ids, tokens = setup
    response = client.post("/api/v1/rules",
                           headers=headers(tokens, "admin"), json={
        "source_id": str(ids["source"]), "rule_key": "broad-same",
        "priority": 100, "day_window": 2, "tolerance_minor": 0,
        "require_reference": False,
    })
    assert response.status_code == 201, response.text
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("TWO-EXTERNAL-A", "2026-09-12", "31.01"),
        ("TWO-EXTERNAL-B", "2026-09-12", "31.01"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("TWO-EXTERNAL-A", "2026-09-12", "31.01"))).status_code == 200
    job = client.post("/api/v1/reconciliation/run",
                      headers=headers(tokens, key="multi-reverse-00700001"),
                      params={"period_id": str(ids["period"])})
    assert job.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == 0
        issues = db.scalars(select(ReconcileIssue).where(
            ReconcileIssue.period_id == ids["period"],
            ReconcileIssue.issue_type == "ambiguous")).all()
        assert len(issues) == 2
        assert all(len(issue.candidates) == 2 for issue in issues)


def test_v070_broad_rule_keeps_tolerance_date_and_high_risk_controls(setup):
    from app.models import MatchGroup, ReconcileIssue
    ids, tokens = setup
    with SessionLocal.begin() as db:
        db.get(MatchRule, ids["rule"]).current = False
    response = client.post("/api/v1/rules",
                           headers=headers(tokens, "admin"), json={
        "source_id": str(ids["source"]), "rule_key": "broad-with-tolerance",
        "priority": 70, "day_window": 1, "tolerance_minor": 5,
        "require_reference": False,
    })
    assert response.status_code == 201, response.text
    assert import_rows(ids, tokens, "external", fixture_lines(
        ("BROAD-HIGH-A", "2026-09-12", "1500.00"),
        ("BROAD-LATE-A", "2026-09-12", "44.00"),
        ("BROAD-AMOUNT-A", "2026-09-14", "33.00"))).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("BROAD-HIGH-B", "2026-09-12", "1499.98"),
        ("BROAD-LATE-B", "2026-09-14", "44.00"),
        ("BROAD-AMOUNT-B", "2026-09-14", "32.94"))).status_code == 200
    job = client.post("/api/v1/reconciliation/run",
                      headers=headers(tokens, key="multi-broad-risk-0070001"),
                      params={"period_id": str(ids["period"])})
    assert job.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        groups = db.scalars(select(MatchGroup).where(
            MatchGroup.period_id == ids["period"])).all()
        assert len(groups) == 1
        group = groups[0]
        assert group.state == "proposed" and group.delta_minor == 2
        assert group.explanation["require_reference"] is False
        assert group.explanation["tolerance_minor"] == 5
        assert db.scalar(select(func.count(ReconcileIssue.id)).where(
            ReconcileIssue.period_id == ids["period"],
            ReconcileIssue.state == "open")) == 2
        assert db.scalar(select(func.count(MatchMember.id)).where(
            MatchMember.group_id == group.id,
            MatchMember.active.is_(True))) == 0
    rejected = client.post(f"/api/v1/match-groups/{group.id}/approve",
                           headers=headers(tokens), json={"expected_version": 1})
    assert rejected.status_code == 403
    confirmed = client.post(f"/api/v1/match-groups/{group.id}/approve",
                            headers=headers(tokens, "controller"),
                            json={"expected_version": 1})
    assert confirmed.status_code == 200



def test_v070_capped_reverse_candidates_mark_all_ambiguous_external_rows(setup):
    """A reverse scan capped at 11 must not hide the 12th/15th external issue."""
    from app.models import MatchGroup, ReconcileIssue

    ids, tokens = setup
    with SessionLocal.begin() as db:
        db.get(MatchRule, ids["rule"]).current = False
    response = client.post("/api/v1/rules",
                           headers=headers(tokens, "admin"), json={
        "source_id": str(ids["source"]), "rule_key": "broad-cap",
        "priority": 80, "day_window": 2, "tolerance_minor": 0,
        "require_reference": False,
    })
    assert response.status_code == 201, response.text
    references = [f"CAP-EXT-{i:02d}" for i in range(15)]
    assert import_rows(ids, tokens, "external", fixture_lines(*[
        (ref, "2026-09-12", "21.02") for ref in references
    ])).status_code == 200
    assert import_rows(ids, tokens, "internal", fixture_lines(
        ("CAP-INTERNAL", "2026-09-12", "21.02"))).status_code == 200
    submitted = client.post("/api/v1/reconciliation/run",
                            headers=headers(tokens, key="broad-overcap-00700001"),
                            params={"period_id": str(ids["period"])})
    assert submitted.status_code == 202
    assert run_once()
    with SessionLocal() as db:
        assert db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == ids["period"])) == 0
        issues = db.scalars(select(ReconcileIssue).where(
            ReconcileIssue.period_id == ids["period"],
            ReconcileIssue.issue_type == "ambiguous")).all()
        assert len(issues) == 15
        issue_ids = {issue.record_id for issue in issues}
        externals = db.scalars(select(Record.id).where(
            Record.period_id == ids["period"],
            Record.kind == "external")).all()
        assert issue_ids == set(externals)
