"""Versioned HTTP API. All financial state changes execute in database transactions."""
import csv
import hashlib
import io
from contextlib import contextmanager
import logging
import os
import time
import re
import uuid
from datetime import date
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from app.db import SessionLocal, get_db
from app.importer import MAX_BYTES, inspect_file
from app.models import (
    AuditEvent, AuthAttempt, ImportBatch, ImportProfile, ImportRejection,
    IssueAssignment, MatchGroup, MatchMember, PeriodSnapshot,
    MatchRule, Period, Record, ReconcileIssue, ReconcileJob, Source,
    SourceGrant, User,
)
from app.request_context import request_id
from app.observability import configure_logging, metrics
from app.security import (
    current_user, ensure_source, hash_password, require_role,
    token_for, verify_password, ROLES,
)
from app.service import (
    approve_group, audit, build_report, cancel_group, close_period,
    create_manual_group, current_rules, locked_period, reopen_period, submit_job,
)
from app.storage import get_original, put_original

def valid_trusted_hosts(raw):
    """Parse explicit deployment hosts; '*' defeats Host-header validation."""
    items = [item.strip() for item in raw.split(",")]
    if not items or any(
        not item or item == "*" or
        not re.fullmatch(r"(?:\*\.)?[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*", item)
        for item in items
    ):
        raise ValueError("TRUSTED_HOSTS must list valid, explicit hostnames")
    return items


def safe_csv_cell(value):
    """Prevent spreadsheet formula evaluation, including leading whitespace."""
    value = str(value)
    if value.lstrip(" \t\r\n\f\v")[:1] in ("=", "+", "-", "@"):
        return "'" + value
    return value


@contextmanager
def transaction(db):
    try:
        yield
        db.commit()
    except Exception:
        db.rollback()
        raise


app = FastAPI(
    title="ReconcileX API", version="0.2.0",
    description="Auditable reconciliation for synthetic bank statements and internal ledgers.",
)
log = configure_logging()
if os.getenv("TRUSTED_HOSTS"):
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=valid_trusted_hosts(os.environ["TRUSTED_HOSTS"]),
    )


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    value = request.headers.get("X-Request-ID")
    identifier = value if value and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value) else str(uuid.uuid4())
    token = request_id.set(identifier)
    started = time.perf_counter()
    try:
        response = await call_next(request)
        response.headers["X-Request-ID"] = identifier
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("Cache-Control", "no-store")
        elapsed = round((time.perf_counter() - started) * 1000, 3)
        route = request.scope.get("route")
        label = request.method + " " + (route.path if route else request.url.path)
        metrics.record(label, response.status_code, elapsed)
        log.info("request", extra={
            "request_id": identifier, "method": request.method,
            "path": label, "status_code": response.status_code,
            "duration_ms": elapsed,
        })
        return response
    finally:
        request_id.reset(token)


def error_body(status, message, details=None):
    return {
        "code": str(status), "message": str(message),
        "requestId": request_id.get(), "details": details or [],
    }


@app.exception_handler(HTTPException)
async def http_error(request, exc):
    detail = exc.detail
    if isinstance(detail, dict):
        return JSONResponse(status_code=exc.status_code, content=error_body(
            exc.status_code, detail.get("reason", "Request rejected"), detail.get("errors", [])))
    return JSONResponse(status_code=exc.status_code, content=error_body(exc.status_code, detail),
                        headers=exc.headers or {})


@app.exception_handler(RequestValidationError)
async def validation_error(request, exc):
    details = [{"field": ".".join(map(str, e["loc"])), "message": e["msg"]}
               for e in exc.errors()]
    return JSONResponse(status_code=422, content=error_body(422, "Validation failed", details))


@app.exception_handler(IntegrityError)
async def constraint_error(request, exc):
    log.warning("constraint_conflict", extra={"request_id": request_id.get()})
    return JSONResponse(status_code=409, content=error_body(
        409, "Concurrent update or unique constraint conflict"))


@app.exception_handler(Exception)
async def unexpected_error(request, exc):
    log.exception("unhandled_request_error", extra={"request_id": request_id.get()})
    return JSONResponse(status_code=500, content=error_body(500, "Internal server error"))


def as_id(value):
    return str(value)


def profile_and_source(db, user, source_id, profile_id):
    source = db.get(Source, source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    ensure_source(db, user, source.id)
    profile = db.get(ImportProfile, profile_id)
    if not profile or profile.source_id != source.id:
        raise HTTPException(404, "Profile not found for source")
    return source, profile


def record_info(r):
    return {
        "id": str(r.id), "source_id": str(r.source_id), "batch_id": str(r.batch_id),
        "kind": r.kind, "reference": r.reference, "account": r.account,
        "currency": r.currency, "booked_date": r.booked_date.isoformat(),
        "amount_minor": r.amount_minor, "row_number": r.row_number,
        "raw_text": r.raw_text,
    }


def group_info(group):
    return {
        "id": str(group.id), "state": group.state, "version": group.version,
        "period_id": str(group.period_id), "source_id": str(group.source_id),
        "rule_id": str(group.rule_id), "rule_version": group.rule_version,
        "manual": group.manual, "comment": group.comment,
        "proposed_by": str(group.proposed_by) if group.proposed_by else None,
        "approved_by": str(group.approved_by) if group.approved_by else None,
        "external_minor": group.external_minor, "internal_minor": group.internal_minor,
        "delta_minor": group.delta_minor, "explanation": group.explanation,
    }


class LoginBody(BaseModel):
    username: str
    password: str


@app.post("/api/v1/auth/token")
def login(body: LoginBody, request: Request, db=Depends(get_db)):
    """DB-backed 5 failures/60s limit for username + originating connection."""
    key = hashlib.sha256(
        ((request.client.host if request.client else "unknown") + "|" + body.username.lower())
        .encode("utf-8")).hexdigest()
    # PostgreSQL advisory lock makes first-attempt insert safe under concurrency.
    if db.bind.dialect.name == "postgresql":
        lock_key = int.from_bytes(bytes.fromhex(key)[:8], "big", signed=True)
        db.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})
    attempt = db.scalar(select(AuthAttempt).where(AuthAttempt.key == key).with_for_update())
    now = datetime.now(timezone.utc)
    if attempt and (now - attempt.window_start).total_seconds() < 60 and attempt.attempts >= 5:
        raise HTTPException(429, "Too many login attempts. Retry after 60 seconds",
                            headers={"Retry-After": "60"})
    user = db.scalar(select(User).where(User.username == body.username))
    if not user or not user.enabled or not verify_password(body.password, user.password_hash):
        if attempt is None:
            db.add(AuthAttempt(key=key, attempts=1, window_start=now))
        elif (now - attempt.window_start).total_seconds() >= 60:
            attempt.attempts = 1
            attempt.window_start = now
        else:
            attempt.attempts += 1
        db.commit()
        raise HTTPException(401, "Invalid credentials")
    if attempt:
        db.delete(attempt)
    db.commit()
    return {"access_token": token_for(user), "token_type": "bearer", "role": user.role}


class SourceBody(BaseModel):
    name: str = Field(min_length=2, max_length=160)
    account: str = Field(min_length=1, max_length=64)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


@app.post("/api/v1/sources", status_code=201)
def create_source(body: SourceBody, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "admin")
    with transaction(db):
        source = Source(**body.model_dump())
        db.add(source)
        db.flush()
        audit(db, user, "source.created", "Source", source.id, body.model_dump(), source.id)
        return {"id": str(source.id)}


@app.get("/api/v1/sources")
def list_sources(user=Depends(current_user), db=Depends(get_db)):
    stmt = select(Source).order_by(Source.name)
    if user.role != "admin":
        stmt = stmt.join(SourceGrant, SourceGrant.source_id == Source.id).where(
            SourceGrant.user_id == user.id)
    return [{"id": str(s.id), "name": s.name, "account": s.account, "currency": s.currency}
            for s in db.scalars(stmt)]


class UserBody(BaseModel):
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=12, max_length=200)
    role: str

    @field_validator("role")
    @classmethod
    def valid_role(cls, value):
        if value not in ROLES:
            raise ValueError("Unknown role")
        return value


@app.post("/api/v1/admin/users", status_code=201)
def create_user(body: UserBody, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "admin")
    with transaction(db):
        new = User(username=body.username, password_hash=hash_password(body.password), role=body.role)
        db.add(new)
        db.flush()
        audit(db, user, "user.created", "User", new.id, {"username": new.username, "role": new.role})
        return {"id": str(new.id)}


class GrantBody(BaseModel):
    user_id: UUID
    source_id: UUID


@app.post("/api/v1/admin/grants", status_code=201)
def grant_source(body: GrantBody, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "admin")
    with transaction(db):
        if not db.get(User, body.user_id) or not db.get(Source, body.source_id):
            raise HTTPException(404, "User or source not found")
        existing = db.scalar(select(SourceGrant).where(
            SourceGrant.user_id == body.user_id, SourceGrant.source_id == body.source_id))
        if existing:
            return {"id": str(existing.id), "created": False}
        grant = SourceGrant(**body.model_dump())
        db.add(grant)
        db.flush()
        audit(db, user, "source.granted", "SourceGrant", grant.id,
              {"user_id": str(body.user_id)}, body.source_id)
        return {"id": str(grant.id), "created": True}


class ProfileBody(BaseModel):
    source_id: UUID
    name: str = Field(min_length=2, max_length=100)
    encoding: str = "utf-8-sig"
    delimiter: str = ","
    date_format: str = "%Y-%m-%d"
    decimal_separator: Literal[".", ","] = "."
    mapping: dict[str, str]

    @field_validator("mapping")
    @classmethod
    def require_columns(cls, value):
        if any(key not in value for key in ("reference", "amount", "date")):
            raise ValueError("Columns reference, amount and date are mandatory")
        return value


@app.post("/api/v1/profiles", status_code=201)
def create_profile(body: ProfileBody, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "admin")
    with transaction(db):
        if not db.get(Source, body.source_id):
            raise HTTPException(404, "Source not found")
        previous = db.scalar(select(func.max(ImportProfile.version)).where(
            ImportProfile.source_id == body.source_id, ImportProfile.name == body.name))
        obj = ImportProfile(**body.model_dump(), version=(previous or 0) + 1)
        db.add(obj)
        db.flush()
        audit(db, user, "profile.version_created", "ImportProfile", obj.id,
              {"version": obj.version, "name": obj.name}, body.source_id)
        return {"id": str(obj.id), "version": obj.version}


@app.get("/api/v1/profiles")
def list_profiles(source_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "operator", "controller", "admin", "auditor")
    ensure_source(db, user, source_id)
    return [
        {"id": str(p.id), "name": p.name, "version": p.version, "encoding": p.encoding,
         "delimiter": p.delimiter, "date_format": p.date_format, "mapping": p.mapping}
        for p in db.scalars(select(ImportProfile).where(
            ImportProfile.source_id == source_id).order_by(ImportProfile.name, ImportProfile.version))
    ]


class PeriodBody(BaseModel):
    name: str = Field(min_length=2, max_length=60)
    start_date: date
    end_date: date


@app.post("/api/v1/periods", status_code=201)
def create_period(body: PeriodBody, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "admin")
    if body.end_date < body.start_date:
        raise HTTPException(400, "Invalid date range")
    with transaction(db):
        if db.scalar(select(Period.id).where(
            Period.start_date <= body.end_date, Period.end_date >= body.start_date)):
            raise HTTPException(409, "Period overlaps an existing period")
        obj = Period(**body.model_dump())
        db.add(obj)
        db.flush()
        audit(db, user, "period.created", "Period", obj.id)
        return {"id": str(obj.id)}


@app.get("/api/v1/periods")
def list_periods(user=Depends(current_user), db=Depends(get_db)):
    return [{"id": str(p.id), "name": p.name, "status": p.status,
             "start_date": p.start_date.isoformat(), "end_date": p.end_date.isoformat()}
            for p in db.scalars(select(Period).order_by(Period.start_date))]


class RuleBody(BaseModel):
    source_id: UUID
    rule_key: str = Field(min_length=3, max_length=100)
    priority: int = Field(ge=0, le=10000)
    day_window: int = Field(ge=0, le=365)
    tolerance_minor: int = Field(ge=0, le=1000000)
    require_reference: bool = True


@app.post("/api/v1/rules", status_code=201)
def add_rule(body: RuleBody, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "admin")
    with transaction(db):
        if not db.get(Source, body.source_id):
            raise HTTPException(404, "Source not found")
        previous = db.scalars(select(MatchRule).where(
            MatchRule.source_id == body.source_id,
            MatchRule.rule_key == body.rule_key
        ).order_by(MatchRule.version.desc()).with_for_update()).all()
        for item in previous:
            item.current = False
        rule = MatchRule(**body.model_dump(), version=(previous[0].version + 1) if previous else 1)
        db.add(rule)
        db.flush()
        audit(db, user, "rule.version_created", "MatchRule", rule.id,
              {"key": rule.rule_key, "version": rule.version}, rule.source_id)
        return {"id": str(rule.id), "version": rule.version}


@app.get("/api/v1/rules")
def list_rules(source_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "operator", "controller", "admin", "auditor")
    ensure_source(db, user, source_id)
    return [{
        "id": str(r.id), "rule_key": r.rule_key, "version": r.version,
        "priority": r.priority, "day_window": r.day_window,
        "tolerance_minor": r.tolerance_minor, "require_reference": r.require_reference,
        "current": r.current,
    } for r in db.scalars(select(MatchRule).where(
        MatchRule.source_id == source_id).order_by(MatchRule.rule_key, MatchRule.version))]


async def read_file(file):
    data = await file.read(MAX_BYTES + 1)
    if not data or len(data) > MAX_BYTES:
        raise HTTPException(413, "File exceeds 20 MiB or is empty")
    return data


def inspect_or_error(data, filename, profile, source, kind, period):
    try:
        return inspect_file(data, filename, profile, source, kind,
                            period.start_date, period.end_date)
    except (ValueError, UnicodeError, csv.Error, OSError, KeyError) as exc:
        raise HTTPException(422, f"Invalid input file: {exc}")


def retain_rejected_import(db, source_id, period_id, profile_id, user, data, result, errors):
    """Persist diagnostic originals, but never post financial Record rows."""
    old = db.scalar(select(ImportRejection).where(
        ImportRejection.source_id == source_id,
        ImportRejection.file_sha256 == result["sha256"]))
    if old:
        return old
    object_key = f"rejected/{source_id}/{result['sha256']}"
    put_original(object_key, data)
    rejected = ImportRejection(
        source_id=source_id, period_id=period_id, profile_id=profile_id,
        file_sha256=result["sha256"], object_key=object_key,
        filename=result["filename"], errors=errors, created_by=user.id,
    )
    db.add(rejected)
    db.flush()
    for error in errors:
        existing_record = error.get("record_id")
        record_id = UUID(existing_record) if existing_record else None
        issue_type = error.get("type", "validation_error")
        if record_id and db.scalar(select(ReconcileIssue.id).where(
            ReconcileIssue.period_id == period_id,
            ReconcileIssue.record_id == record_id,
            ReconcileIssue.issue_type == issue_type)):
            continue
        db.add(ReconcileIssue(
            period_id=period_id, source_id=source_id, record_id=record_id,
            issue_type=issue_type, state="open", candidates=[{
                "rejected_import_id": str(rejected.id),
                "file_sha256": result["sha256"], "line": error.get("line"),
                "message": error.get("message"),
            }],
        ))
    audit(db, user, "import.rejected", "ImportRejection", rejected.id,
          {"sha256": rejected.file_sha256, "errors": len(errors)}, source_id)
    return rejected


@app.post("/api/v1/imports/preview")
async def import_preview(
    source_id: UUID = Form(...), profile_id: UUID = Form(...),
    period_id: UUID = Form(...), kind: Literal["external", "internal"] = Form(...),
    file: UploadFile = File(...),
    user=Depends(current_user), db=Depends(get_db),
):
    require_role(user, "operator")
    source, profile = profile_and_source(db, user, source_id, profile_id)
    period = db.get(Period, period_id)
    if not period:
        raise HTTPException(404, "Period not found")
    if period.status != "open":
        raise HTTPException(409, "Cannot import into closed period")
    data = await read_file(file)
    result = inspect_or_error(data, file.filename or "", profile, source, kind, period)
    return {
        "sha256": result["sha256"], "valid_rows": result["valid_rows"],
        "total_minor": result["total_minor"], "errors": result["errors"],
        "preview": [r["normalized"] for r in result["records"][:10]],
        "can_commit": not result["errors"],
    }


@app.post("/api/v1/imports/commit")
async def import_commit(
    source_id: UUID = Form(...), profile_id: UUID = Form(...),
    period_id: UUID = Form(...), kind: Literal["external", "internal"] = Form(...),
    file: UploadFile = File(...), user=Depends(current_user), db=Depends(get_db),
):
    require_role(user, "operator")
    source, profile = profile_and_source(db, user, source_id, profile_id)
    period = db.get(Period, period_id)
    if not period:
        raise HTTPException(404, "Period not found")
    data = await read_file(file)
    result = inspect_or_error(data, file.filename or "", profile, source, kind, period)
    if result["errors"]:
        with transaction(db):
            locked_period(db, period_id)
            rejected = retain_rejected_import(
                db, source_id, period_id, profile_id, user, data, result,
                result["errors"])
            return JSONResponse(status_code=422, content=error_body(
                422, "Atomic import rejected: invalid rows", result["errors"]),
                headers={"X-Rejection-ID": str(rejected.id)})
    with transaction(db):
        previous = db.scalar(select(ImportBatch).where(
            ImportBatch.source_id == source_id, ImportBatch.file_sha256 == result["sha256"]))
        if previous:
            if previous.kind != kind or previous.period_id != period_id:
                raise HTTPException(409, "Identical file was imported with a different kind or period")
            return {"id": str(previous.id), "duplicate": True,
                    "row_count": previous.row_count, "total_minor": previous.total_minor}
        locked_period(db, period_id)
        keys = [r["natural_key"] for r in result["records"]]
        for offset in range(0, len(keys), 500):
            existing = db.scalar(select(Record).where(
                Record.source_id == source_id, Record.kind == kind,
                Record.natural_key.in_(keys[offset:offset + 500])).limit(1))
            if existing:
                conflict = next(row for row in result["records"]
                                if row["natural_key"] == existing.natural_key)
                error = {
                    "line": conflict["row_number"], "type": "duplicate",
                    "record_id": str(existing.id),
                    "message": "Natural-key duplicate from an earlier import",
                }
                rejected = retain_rejected_import(
                    db, source_id, period_id, profile_id, user, data, result, [error])
                return JSONResponse(status_code=409, content=error_body(
                    409, "Natural-key duplicate; no financial rows imported", [error]),
                    headers={"X-Rejection-ID": str(rejected.id)})
        object_key = f"originals/{source_id}/{result['sha256']}"
        put_original(object_key, data)  # failure aborts before any financial DB write
        batch = ImportBatch(
            source_id=source_id, profile_id=profile_id, period_id=period_id,
            file_sha256=result["sha256"], object_key=object_key,
            filename=result["filename"], kind=kind, row_count=len(keys),
            total_minor=result["total_minor"], created_by=user.id)
        db.add(batch)
        db.flush()
        for row in result["records"]:
            db.add(Record(
                source_id=source_id, period_id=period_id, batch_id=batch.id,
                kind=kind, natural_key=row["natural_key"],
                reference=row["reference"], account=row["account"],
                currency=row["currency"], amount_minor=row["amount_minor"],
                booked_date=row["booked_date"], counterparty=row["counterparty"],
                row_number=row["row_number"], raw_text=row["raw_text"],
                normalized=row["normalized"],
            ))
        db.flush()
        audit(db, user, "import.committed", "ImportBatch", batch.id, {
            "sha256": batch.file_sha256, "rows": batch.row_count,
            "total_minor": batch.total_minor, "kind": kind,
        }, source_id)
        return {"id": str(batch.id), "duplicate": False,
                "row_count": batch.row_count, "total_minor": batch.total_minor}


@app.get("/api/v1/imports/{batch_id}/file")
def download_original(batch_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    batch = db.get(ImportBatch, batch_id)
    if not batch:
        raise HTTPException(404, "Batch not found")
    try:
        ensure_source(db, user, batch.source_id)
    except HTTPException:
        # Security denial is written in a separate transaction, not rolled back with 403.
        with SessionLocal.begin() as session:
            audit(session, user, "security.file_denied", "ImportBatch", batch.id,
                  {"reason": "source_access_denied"}, batch.source_id)
        raise
    if user.role not in {"operator", "controller", "admin", "auditor"}:
        with SessionLocal.begin() as session:
            audit(session, user, "security.file_denied", "ImportBatch", batch.id,
                  {"reason": "role_forbidden"}, batch.source_id)
        raise HTTPException(403, "Role cannot download originals")
    data = get_original(batch.object_key)
    if hashlib.sha256(data).hexdigest() != batch.file_sha256:
        raise HTTPException(500, "Original file integrity check failed")
    return Response(data, media_type="application/octet-stream",
                    headers={"Content-Disposition": "attachment; filename=original.bin",
                             "Cache-Control": "no-store"})


@app.get("/api/v1/records")
def list_records(
    source_id: UUID, period_id: UUID, kind: Literal["external", "internal"] | None = None,
    limit: int = 50, offset: int = 0,
    user=Depends(current_user), db=Depends(get_db),
):
    require_role(user, "operator", "controller", "auditor")
    ensure_source(db, user, source_id)
    if limit < 1 or limit > 200 or offset < 0:
        raise HTTPException(400, "Invalid pagination")
    stmt = select(Record).where(Record.source_id == source_id, Record.period_id == period_id)
    if kind:
        stmt = stmt.where(Record.kind == kind)
    return [record_info(r) for r in db.scalars(
        stmt.order_by(Record.booked_date, Record.id).limit(limit).offset(offset))]


@app.post("/api/v1/reconciliation/run", status_code=202)
def start_reconciliation(
    period_id: UUID, key: str | None = Header(default=None, alias="Idempotency-Key"),
    user=Depends(current_user), db=Depends(get_db),
):
    with transaction(db):
        job, created = submit_job(db, period_id, key or "", user)
        return {"id": str(job.id), "state": job.state, "created": created,
                "rules_snapshot": job.rules_snapshot}


@app.get("/api/v1/reconciliation/jobs/{job_id}")
def job_status(job_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    job = db.get(ReconcileJob, job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    # Only the submitting user or an administrator can read job metadata.
    if job.created_by != user.id and user.role != "admin":
        raise HTTPException(403, "Job not accessible")
    return {"id": str(job.id), "state": job.state, "attempts": job.attempts, "error": job.error}


@app.post("/api/v1/reconciliation/jobs/{job_id}/retry")
def retry_job(job_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "operator", "controller")
    with transaction(db):
        job = db.scalar(select(ReconcileJob).where(ReconcileJob.id == job_id).with_for_update())
        if not job:
            raise HTTPException(404, "Job not found")
        if job.created_by != user.id:
            raise HTTPException(403, "Only job submitter can retry")
        for rule in job.rules_snapshot:
            ensure_source(db, user, UUID(rule["source_id"]))
        locked_period(db, job.period_id)
        if job.state != "failed":
            raise HTTPException(409, "Only failed jobs may be retried")
        job.state = "queued"
        job.error = None
        job.lease_until = None
        audit(db, user, "job.retried", "ReconcileJob", job.id)
        return {"id": str(job.id), "state": job.state}


@app.get("/api/v1/issues")
def issues(
    source_id: UUID, period_id: UUID, issue_type: str | None = None, state: str = "open",
    limit: int = 50, offset: int = 0, user=Depends(current_user), db=Depends(get_db),
):
    require_role(user, "operator", "controller", "auditor")
    ensure_source(db, user, source_id)
    if limit < 1 or limit > 200 or offset < 0:
        raise HTTPException(400, "Invalid pagination")
    stmt = select(ReconcileIssue).where(
        ReconcileIssue.source_id == source_id, ReconcileIssue.period_id == period_id,
        ReconcileIssue.state == state)
    if issue_type:
        stmt = stmt.where(ReconcileIssue.issue_type == issue_type)
    found = db.scalars(stmt.order_by(ReconcileIssue.created_at)
                       .limit(limit).offset(offset)).all()
    assignments = {a.issue_id: str(a.owner_id) for a in db.scalars(
        select(IssueAssignment).where(IssueAssignment.issue_id.in_([i.id for i in found])))}
    return [{"id": str(i.id), "record_id": str(i.record_id) if i.record_id else None,
             "issue_type": i.issue_type, "state": i.state, "candidates": i.candidates,
             "owner_id": assignments.get(i.id)}
            for i in found]


class GroupBody(BaseModel):
    period_id: UUID
    source_id: UUID
    rule_id: UUID
    external_ids: list[UUID] = Field(min_length=1, max_length=10)
    internal_ids: list[UUID] = Field(min_length=1, max_length=10)
    comment: str = Field(min_length=5, max_length=4000)


@app.post("/api/v1/match-groups", status_code=201)
def propose_match(
    body: GroupBody, key: str | None = Header(default=None, alias="Idempotency-Key"),
    user=Depends(current_user), db=Depends(get_db),
):
    with transaction(db):
        group = create_manual_group(
            db, body.period_id, body.source_id, body.external_ids,
            body.internal_ids, body.rule_id, user, body.comment, key,
        )
        return group_info(group)


class DecisionBody(BaseModel):
    expected_version: int = Field(ge=1)


@app.post("/api/v1/match-groups/{group_id}/approve")
def approve(group_id: UUID, body: DecisionBody,
            user=Depends(current_user), db=Depends(get_db)):
    with transaction(db):
        return group_info(approve_group(db, group_id, user, body.expected_version))


class CancelBody(DecisionBody):
    reason: str = Field(min_length=10, max_length=4000)


@app.post("/api/v1/match-groups/{group_id}/cancel")
def cancel(group_id: UUID, body: CancelBody,
           user=Depends(current_user), db=Depends(get_db)):
    with transaction(db):
        return group_info(cancel_group(db, group_id, user, body.reason, body.expected_version))


@app.get("/api/v1/match-groups/{group_id}")
def get_match(group_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    group = db.get(MatchGroup, group_id)
    if not group:
        raise HTTPException(404, "Group not found")
    require_role(user, "operator", "controller", "auditor")
    ensure_source(db, user, group.source_id)
    member_ids = db.scalars(select(MatchMember.record_id).where(
        MatchMember.group_id == group_id)).all()
    records = db.scalars(select(Record).where(Record.id.in_(member_ids))).all()
    info = group_info(group)
    info["original_rows"] = [record_info(r) for r in records]
    return info


@app.post("/api/v1/periods/{period_id}/close")
def close(period_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    with transaction(db):
        # A controller cannot close a period containing sources outside their grants.
        sources = db.scalars(select(ImportBatch.source_id).where(
            ImportBatch.period_id == period_id).distinct()).all()
        for source_id in sources:
            ensure_source(db, user, source_id)
        period = close_period(db, period_id, user)
        return {"id": str(period.id), "status": period.status,
                "snapshot_sha256": period.snapshot_sha256}


class ReasonBody(BaseModel):
    reason: str = Field(min_length=10, max_length=4000)


@app.post("/api/v1/periods/{period_id}/reopen")
def reopen(period_id: UUID, body: ReasonBody, user=Depends(current_user), db=Depends(get_db)):
    with transaction(db):
        period = reopen_period(db, period_id, user, body.reason)
        return {"id": str(period.id), "status": period.status, "version": period.version}


@app.get("/api/v1/periods/{period_id}/snapshots")
def historical_snapshots(period_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "auditor", "controller", "admin")
    if not db.get(Period, period_id):
        raise HTTPException(404, "Period not found")
    histories = db.scalars(select(PeriodSnapshot).where(
        PeriodSnapshot.period_id == period_id).order_by(PeriodSnapshot.sequence)).all()
    if user.role == "admin":
        allowed = None
    else:
        allowed = set(db.scalars(select(SourceGrant.source_id).where(
            SourceGrant.user_id == user.id)).all())
    output = []
    for h in histories:
        report = h.content["report"]
        rows = report["by_source"]
        if allowed is not None:
            rows = [r for r in rows if UUID(r["source_id"]) in allowed]
        if rows or allowed is None:
            output.append({
                "sequence": h.sequence, "snapshot_sha256": h.sha256,
                "closed_by": str(h.closed_by),
                "report": {"period_id": report["period_id"],
                           "period_name": report["period_name"], "by_source": rows},
            })
    return output


@app.get("/api/v1/reports/period/{period_id}")
def period_report(period_id: UUID, user=Depends(current_user), db=Depends(get_db)):
    period = db.get(Period, period_id)
    if not period:
        raise HTTPException(404, "Period not found")
    report = period.snapshot["report"] if period.status == "closed" and period.snapshot else build_report(db, period)
    if user.role == "admin":
        allowed = None
    else:
        allowed = set(db.scalars(select(SourceGrant.source_id).where(
            SourceGrant.user_id == user.id)).all())
    rows = report["by_source"]
    if allowed is not None:
        rows = [r for r in rows if UUID(r["source_id"]) in allowed]
    return {
        "period_id": report["period_id"], "period_name": report["period_name"],
        "status": period.status, "snapshot_sha256": period.snapshot_sha256
        if period.status == "closed" else None, "by_source": rows,
    }


@app.get("/api/v1/exports/records.csv")
def export_records(
    source_id: UUID, period_id: UUID, user=Depends(current_user), db=Depends(get_db),
):
    require_role(user, "operator", "controller", "auditor")
    ensure_source(db, user, source_id)
    records = db.scalars(select(Record).where(
        Record.source_id == source_id, Record.period_id == period_id
    ).order_by(Record.booked_date, Record.id)).all()
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(["id", "kind", "reference", "date", "account", "currency", "amount_minor"])
    for r in records:
        writer.writerow([str(r.id), r.kind, safe_csv_cell(r.reference), r.booked_date.isoformat(),
                         safe_csv_cell(r.account), r.currency, r.amount_minor])
    return Response(stream.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": "attachment; filename=records.csv"})


@app.get("/api/v1/audit")
def audit_history(
    source_id: UUID, limit: int = 100, user=Depends(current_user), db=Depends(get_db),
):
    require_role(user, "auditor", "controller", "admin")
    ensure_source(db, user, source_id)
    if not 1 <= limit <= 500:
        raise HTTPException(400, "Invalid limit")
    events = db.scalars(select(AuditEvent).where(
        AuditEvent.source_id == source_id
    ).order_by(AuditEvent.created_at.desc()).limit(limit)).all()
    return [{"id": str(e.id), "action": e.action, "entity_type": e.entity_type,
             "entity_id": e.entity_id, "actor_id": str(e.actor_id) if e.actor_id else None,
             "details": e.details, "at": e.created_at.isoformat()}
            for e in events]


@app.get("/health/live")
def live():
    return {"status": "ok"}


@app.get("/health/ready")
def ready(db=Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"status": "ready"}


@app.get("/api/v1/admin/metrics")
def request_metrics(user=Depends(current_user)):
    require_role(user, "admin")
    return {
        "scope": "single API process", "percentile_window": 10000,
        "routes": metrics.snapshot(),
    }


class AssignIssueBody(BaseModel):
    owner_id: UUID


@app.post("/api/v1/issues/{issue_id}/assign")
def assign_issue(issue_id: UUID, body: AssignIssueBody,
                 user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "operator", "controller")
    with transaction(db):
        issue = db.scalar(select(ReconcileIssue).where(
            ReconcileIssue.id == issue_id).with_for_update())
        if not issue:
            raise HTTPException(404, "Issue not found")
        ensure_source(db, user, issue.source_id)
        locked_period(db, issue.period_id)
        owner = db.get(User, body.owner_id)
        if not owner or owner.role != "operator" or not owner.enabled:
            raise HTTPException(400, "Issue owner must be an enabled operator")
        ensure_source(db, owner, issue.source_id)
        assignment = db.scalar(select(IssueAssignment).where(
            IssueAssignment.issue_id == issue_id).with_for_update())
        if assignment:
            assignment.owner_id = owner.id
            assignment.assigned_by = user.id
        else:
            assignment = IssueAssignment(
                issue_id=issue_id, owner_id=owner.id, assigned_by=user.id)
            db.add(assignment)
        audit(db, user, "issue.assigned", "ReconcileIssue", issue.id,
              {"owner_id": str(owner.id)}, issue.source_id)
        return {"id": str(issue.id), "owner_id": str(owner.id)}


class ResolveIssueBody(BaseModel):
    reason: str = Field(min_length=10, max_length=4000)


@app.post("/api/v1/issues/{issue_id}/resolve")
def resolve_issue(issue_id: UUID, body: ResolveIssueBody,
                  user=Depends(current_user), db=Depends(get_db)):
    require_role(user, "operator", "controller")
    with transaction(db):
        issue = db.scalar(select(ReconcileIssue).where(
            ReconcileIssue.id == issue_id).with_for_update())
        if not issue:
            raise HTTPException(404, "Issue not found")
        ensure_source(db, user, issue.source_id)
        locked_period(db, issue.period_id)
        if issue.state != "open":
            raise HTTPException(409, "Issue already resolved")
        issue.state = "resolved"
        audit(db, user, "issue.resolved", "ReconcileIssue", issue.id,
              {"reason": body.reason}, issue.source_id)
        return {"id": str(issue.id), "state": issue.state}


@app.get("/api/v1/imports/rejections/{rejection_id}/file")
def get_rejected_original(rejection_id: UUID, user=Depends(current_user),
                          db=Depends(get_db)):
    item = db.get(ImportRejection, rejection_id)
    if not item:
        raise HTTPException(404, "Rejected import not found")
    try:
        ensure_source(db, user, item.source_id)
        require_role(user, "operator", "controller", "admin", "auditor")
    except HTTPException:
        with SessionLocal.begin() as session:
            audit(session, user, "security.file_denied", "ImportRejection", item.id,
                  {"reason": "access_denied"}, item.source_id)
        raise
    data = get_original(item.object_key)
    if hashlib.sha256(data).hexdigest() != item.file_sha256:
        raise HTTPException(500, "Rejected original checksum mismatch")
    return Response(data, media_type="application/octet-stream",
                    headers={"Cache-Control": "no-store",
                             "Content-Disposition": "attachment; filename=rejected.bin"})
