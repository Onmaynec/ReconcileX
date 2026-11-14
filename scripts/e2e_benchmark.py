"""End-to-end HTTP + PostgreSQL + worker performance acceptance.

Only synthetic records. Refuses to write into a non-test database unless
ALLOW_BENCHMARK_WRITES=1 is explicitly set.
"""
import argparse
import hashlib
import json
import os
import platform
import time
import uuid
from datetime import date

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.api import app
from app.db import SessionLocal
from app.models import (
    ImportProfile, MatchGroup, MatchMember, MatchRule, Period,
    Record, ReconcileIssue, ReconcileJob, Source, SourceGrant, User,
)
from app.security import token_for
from app.worker import run_once
from scripts.benchmark import host_details, memory_mib


def make_file(kind, pairs, internal_extra, external_extra):
    """A pair shares reference/date/amount, but bytes differ by origin field."""
    rows = ["reference,date,amount,origin\n"]
    count = pairs + (internal_extra if kind == "internal" else external_extra)
    for i in range(count):
        ref = f"PAIR-{i:08d}" if i < pairs else f"{kind.upper()}-ONLY-{i:08d}"
        rows.append(f"{ref},2099-01-{(i % 28) + 1:02d},10.00,{kind}\n")
    return "".join(rows).encode("utf-8")


def post_file(client, path, token, source, profile, period, kind, content):
    return client.post(
        "/api/v1/imports/" + path,
        headers={"Authorization": "Bearer " + token},
        data={"source_id": str(source), "profile_id": str(profile),
              "period_id": str(period), "kind": kind},
        files={"file": (kind + ".csv", content, "text/csv")},
    )


def checked(response, status):
    if response.status_code != status:
        raise RuntimeError(
            f"HTTP {response.status_code} != {status}: {response.text[:1200]}")
    return response.json()


def run_benchmark(pairs=80000, internal_extra=20000, external_extra=0,
                  repeat_import=True):
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        raise RuntimeError("DATABASE_URL is required")
    name = db_url.rsplit("/", 1)[-1].split("?", 1)[0]
    if not name.endswith("_test") and os.getenv("ALLOW_BENCHMARK_WRITES") != "1":
        raise RuntimeError("Refusing non-test database; set ALLOW_BENCHMARK_WRITES=1 explicitly")
    if min(pairs, internal_extra, external_extra) < 0 or pairs < 1:
        raise ValueError("At least one pair and nonnegative extra rows required")
    if max(pairs + internal_extra, pairs + external_extra) > 250000:
        raise ValueError("Maximum 250000 rows per file")
    suffix = uuid.uuid4().hex[:12]
    timeline = {}
    start = time.perf_counter()
    with SessionLocal.begin() as db:
        operator = db.scalar(select(User).where(User.username == "operator"))
        controller = db.scalar(select(User).where(User.username == "controller"))
        if not operator or not controller:
            raise RuntimeError("Seed the two demo roles on the test DB first")
        source = Source(name="perf-" + suffix, account="SYNTH-" + suffix,
                        currency="RUB")
        period = Period(name="perf-" + suffix, start_date=date(2099, 1, 1),
                        end_date=date(2099, 1, 31))
        db.add_all((source, period))
        db.flush()
        for user in (operator, controller):
            db.add(SourceGrant(source_id=source.id, user_id=user.id))
        profile = ImportProfile(
            source_id=source.id, name="perf-csv", version=1,
            encoding="utf-8", delimiter=",", date_format="%Y-%m-%d",
            decimal_separator=".",
            mapping={"reference": "reference", "date": "date", "amount": "amount"},
        )
        rule = MatchRule(
            source_id=source.id, rule_key="exact-perf", version=1,
            priority=100, day_window=2, tolerance_minor=0,
            require_reference=True,
        )
        db.add_all((profile, rule))
        db.flush()
        source_id, period_id, profile_id = source.id, period.id, profile.id
        operator_token = token_for(operator)
        controller_token = token_for(controller)
    timeline["setup_ms"] = round((time.perf_counter() - start) * 1000, 3)
    client = TestClient(app)

    inputs = {
        kind: make_file(kind, pairs, internal_extra, external_extra)
        for kind in ("external", "internal")
    }
    preview = {}
    imports = {}
    for kind, data in inputs.items():
        t0 = time.perf_counter()
        pr = checked(post_file(client, "preview", operator_token, source_id,
                               profile_id, period_id, kind, data), 200)
        preview[kind] = {
            "duration_ms": round((time.perf_counter() - t0) * 1000, 3),
            "rows": pr["valid_rows"], "total_minor": pr["total_minor"],
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        if not pr["can_commit"] or pr["errors"]:
            raise RuntimeError("Synthetic file failed preview")
        t0 = time.perf_counter()
        result = checked(post_file(client, "commit", operator_token, source_id,
                                   profile_id, period_id, kind, data), 200)
        if result["duplicate"]:
            raise RuntimeError("First import unexpectedly marked duplicate")
        imports[kind] = {
            "duration_ms": round((time.perf_counter() - t0) * 1000, 3),
            "row_count": result["row_count"],
            "total_minor": result["total_minor"],
        }
    timeline["preview"] = preview
    timeline["import"] = imports

    if repeat_import:
        repeated = {}
        for kind, data in inputs.items():
            t0 = time.perf_counter()
            result = checked(post_file(client, "commit", operator_token, source_id,
                                       profile_id, period_id, kind, data), 200)
            if not result["duplicate"]:
                raise RuntimeError("Repeat import created a second financial batch")
            repeated[kind] = round((time.perf_counter() - t0) * 1000, 3)
        timeline["duplicate_import_ms"] = repeated

    t0 = time.perf_counter()
    submitted = checked(
        client.post(
            "/api/v1/reconciliation/run",
            params={"period_id": str(period_id)},
            headers={
                "Authorization": "Bearer " + operator_token,
                "Idempotency-Key": "perf-" + suffix,
            },
        ), 202,
    )
    job_id = uuid.UUID(submitted["id"])
    timeline["enqueue_ms"] = round((time.perf_counter() - t0) * 1000, 3)

    t0 = time.perf_counter()
    for iteration in range(20):
        if not run_once():
            break
        with SessionLocal() as db:
            job_state = db.get(ReconcileJob, job_id).state
        if job_state in ("done", "failed"):
            break
    timeline["reconcile_ms"] = round((time.perf_counter() - t0) * 1000, 3)
    with SessionLocal() as db:
        job = db.get(ReconcileJob, job_id)
        if job.state != "done":
            raise RuntimeError(f"Worker job {job.state}: {job.error}")
        group_count = db.scalar(select(func.count(MatchGroup.id)).where(
            MatchGroup.period_id == period_id,
            MatchGroup.source_id == source_id,
            MatchGroup.state == "approved",
        ))
        members = db.scalar(select(func.count(MatchMember.id)).join(
            MatchGroup, MatchMember.group_id == MatchGroup.id).where(
                MatchGroup.period_id == period_id,
                MatchGroup.source_id == source_id,
                MatchMember.active.is_(True),
            ))
        records = db.execute(select(
            Record.kind, func.count(Record.id), func.sum(Record.amount_minor),
        ).where(
            Record.period_id == period_id,
            Record.source_id == source_id,
        ).group_by(Record.kind)).all()
        issues = db.scalar(select(func.count(ReconcileIssue.id)).where(
            ReconcileIssue.period_id == period_id,
            ReconcileIssue.source_id == source_id,
            ReconcileIssue.state == "open",
        ))
        if group_count != pairs or members != pairs * 2:
            raise RuntimeError(
                f"Financial match invariant failed: groups={group_count}, members={members}")
        expected = {"external": pairs + external_extra,
                    "internal": pairs + internal_extra}
        for kind, count, amount in records:
            if count != expected[kind] or amount != count * 1000:
                raise RuntimeError("Record count/minor-unit total mismatch")
        if issues != external_extra:
            raise RuntimeError(f"Expected {external_extra} unmatched external, got {issues}")
    timeline["verified"] = {
        "approved_groups": group_count, "active_members": members,
        "open_issues": issues,
        "totals_minor": {kind: int(total) for kind, _, total in records},
        "row_counts": {kind: count for kind, count, _ in records},
    }

    t0 = time.perf_counter()
    closing = checked(
        client.post(f"/api/v1/periods/{period_id}/close",
                    headers={"Authorization": "Bearer " + controller_token}),
        200,
    )
    timeline["close_ms"] = round((time.perf_counter() - t0) * 1000, 3)
    with SessionLocal() as db:
        persisted = db.get(Period, period_id)
        if persisted.status != "closed" or not persisted.snapshot_sha256:
            raise RuntimeError("Period was not closed with checksum")
        timeline["snapshot_sha256"] = persisted.snapshot_sha256
    timeline["wall_ms"] = round((time.perf_counter() - start) * 1000, 3)
    return {
        "scenario": "PV-FIN-03-e2e", "synthetic": True,
        "run_id": suffix, "host": host_details(),
        "memory_peak_mib": memory_mib(),
        "external_rows": pairs + external_extra,
        "internal_rows": pairs + internal_extra,
        "pairs": pairs, "results": timeline,
    }


def main():
    parser = argparse.ArgumentParser(description="End-to-end 1:1 reconciliation benchmark")
    parser.add_argument("--pairs", type=int, default=80000)
    parser.add_argument("--internal-extra", type=int, default=20000)
    parser.add_argument("--external-extra", type=int, default=0)
    parser.add_argument("--no-repeat", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    result = run_benchmark(args.pairs, args.internal_extra, args.external_extra,
                           not args.no_repeat)
    data = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        from pathlib import Path
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(data + "\n", encoding="utf-8")
    print(data)


if __name__ == "__main__":
    main()
