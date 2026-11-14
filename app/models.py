import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, CheckConstraint, Column, Date, DateTime, ForeignKey, Index, Integer,
    BigInteger, JSON, String, Text, UniqueConstraint, Uuid, text,
)
from sqlalchemy.orm import relationship

from app.db import Base


def uid():
    return uuid.uuid4()


def utcnow():
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(20), nullable=False)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)


class Source(Base):
    __tablename__ = "data_sources"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    name = Column(String(160), nullable=False, unique=True)
    account = Column(String(64), nullable=False)
    currency = Column(String(3), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)


class SourceGrant(Base):
    __tablename__ = "source_grants"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    user_id = Column(Uuid(as_uuid=True), ForeignKey("users.id"), nullable=False)
    source_id = Column(Uuid(as_uuid=True), ForeignKey("data_sources.id"), nullable=False)
    __table_args__ = (UniqueConstraint("user_id", "source_id", name="uq_source_grant"),)


class ImportProfile(Base):
    __tablename__ = "import_profiles"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    source_id = Column(Uuid(as_uuid=True), ForeignKey("data_sources.id"), nullable=False)
    name = Column(String(100), nullable=False)
    encoding = Column(String(32), nullable=False, default="utf-8-sig")
    delimiter = Column(String(2), nullable=False, default=",")
    date_format = Column(String(50), nullable=False, default="%Y-%m-%d")
    decimal_separator = Column(String(1), nullable=False, default=".")
    mapping = Column(JSON, nullable=False)
    version = Column(Integer, nullable=False, default=1)
    __table_args__ = (UniqueConstraint("source_id", "name", "version", name="uq_profile_version"),)


class Period(Base):
    __tablename__ = "periods"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    name = Column(String(60), nullable=False, unique=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    status = Column(String(12), nullable=False, default="open")
    snapshot = Column(JSON, nullable=True)
    snapshot_sha256 = Column(String(64), nullable=True)
    closed_by = Column(Uuid(as_uuid=True), ForeignKey("users.id"), nullable=True)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    version = Column(Integer, nullable=False, default=1)
    __table_args__ = (CheckConstraint("start_date <= end_date", name="ck_period_dates"),)


class ImportBatch(Base):
    __tablename__ = "import_batches"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    source_id = Column(Uuid(as_uuid=True), ForeignKey("data_sources.id"), nullable=False)
    period_id = Column(Uuid(as_uuid=True), ForeignKey("periods.id"), nullable=False)
    profile_id = Column(Uuid(as_uuid=True), ForeignKey("import_profiles.id"), nullable=False)
    file_sha256 = Column(String(64), nullable=False)
    object_key = Column(String(240), nullable=False)
    filename = Column(String(240), nullable=False)
    kind = Column(String(8), nullable=False)
    row_count = Column(Integer, nullable=False)
    total_minor = Column(BigInteger, nullable=False)
    created_by = Column(Uuid(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    status = Column(String(16), nullable=False, default="committed")
    __table_args__ = (UniqueConstraint("source_id", "file_sha256", name="uq_source_file"),)


class Record(Base):
    __tablename__ = "records"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    source_id = Column(Uuid(as_uuid=True), ForeignKey("data_sources.id"), nullable=False)
    period_id = Column(Uuid(as_uuid=True), ForeignKey("periods.id"), nullable=False)
    batch_id = Column(Uuid(as_uuid=True), ForeignKey("import_batches.id"), nullable=False)
    kind = Column(String(8), nullable=False)
    natural_key = Column(String(64), nullable=False)
    reference = Column(String(160), nullable=False)
    account = Column(String(64), nullable=False)
    currency = Column(String(3), nullable=False)
    amount_minor = Column(BigInteger, nullable=False)
    booked_date = Column(Date, nullable=False)
    counterparty = Column(String(180), nullable=True)
    row_number = Column(Integer, nullable=False)
    raw_text = Column(Text, nullable=False)
    normalized = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    __table_args__ = (
        UniqueConstraint("source_id", "kind", "natural_key", name="uq_natural_record"),
        Index("ix_record_lookup", "period_id", "kind", "account", "currency", "reference", "amount_minor"),
    )


class MatchRule(Base):
    __tablename__ = "match_rules"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    source_id = Column(Uuid(as_uuid=True), ForeignKey("data_sources.id"), nullable=False)
    rule_key = Column(String(100), nullable=False)
    version = Column(Integer, nullable=False)
    priority = Column(Integer, nullable=False)
    day_window = Column(Integer, nullable=False, default=2)
    tolerance_minor = Column(Integer, nullable=False, default=0)
    require_reference = Column(Boolean, nullable=False, default=True)
    current = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    __table_args__ = (
        UniqueConstraint("source_id", "rule_key", "version", name="uq_rule_version"),
        CheckConstraint("day_window >= 0 AND tolerance_minor >= 0", name="ck_rule_bounds"),
    )


class MatchGroup(Base):
    __tablename__ = "match_groups"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    period_id = Column(Uuid(as_uuid=True), ForeignKey("periods.id"), nullable=False)
    source_id = Column(Uuid(as_uuid=True), ForeignKey("data_sources.id"), nullable=False)
    rule_id = Column(Uuid(as_uuid=True), ForeignKey("match_rules.id"), nullable=False)
    rule_version = Column(Integer, nullable=False)
    state = Column(String(16), nullable=False, default="proposed")
    manual = Column(Boolean, nullable=False, default=False)
    proposed_by = Column(Uuid(as_uuid=True), ForeignKey("users.id"), nullable=True)
    approved_by = Column(Uuid(as_uuid=True), ForeignKey("users.id"), nullable=True)
    comment = Column(Text, nullable=True)
    explanation = Column(JSON, nullable=False)
    external_minor = Column(BigInteger, nullable=False)
    internal_minor = Column(BigInteger, nullable=False)
    delta_minor = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    approved_at = Column(DateTime(timezone=True), nullable=True)
    version = Column(Integer, nullable=False, default=1)
    members = relationship("MatchMember", cascade="all, delete-orphan")


class MatchMember(Base):
    __tablename__ = "match_members"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    group_id = Column(Uuid(as_uuid=True), ForeignKey("match_groups.id"), nullable=False)
    record_id = Column(Uuid(as_uuid=True), ForeignKey("records.id"), nullable=False)
    active = Column(Boolean, nullable=False, default=False)
    __table_args__ = (
        UniqueConstraint("group_id", "record_id", name="uq_group_record"),
        Index(
            "uq_active_record", "record_id", unique=True,
            postgresql_where=text("active = true"), sqlite_where=text("active = 1"),
        ),
    )


class ReconcileIssue(Base):
    __tablename__ = "reconcile_issues"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    period_id = Column(Uuid(as_uuid=True), ForeignKey("periods.id"), nullable=False)
    source_id = Column(Uuid(as_uuid=True), ForeignKey("data_sources.id"), nullable=False)
    record_id = Column(Uuid(as_uuid=True), ForeignKey("records.id"), nullable=True)
    issue_type = Column(String(40), nullable=False)
    candidates = Column(JSON, nullable=False, default=list)
    state = Column(String(12), nullable=False, default="open")
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    __table_args__ = (UniqueConstraint("period_id", "record_id", "issue_type", name="uq_issue_record_type"),)


class ReconcileJob(Base):
    __tablename__ = "reconcile_jobs"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    period_id = Column(Uuid(as_uuid=True), ForeignKey("periods.id"), nullable=False)
    idempotency_key = Column(String(120), nullable=False)
    rules_snapshot = Column(JSON, nullable=False)
    state = Column(String(16), nullable=False, default="queued")
    attempts = Column(Integer, nullable=False, default=0)
    lease_until = Column(DateTime(timezone=True), nullable=True)
    error = Column(Text, nullable=True)
    created_by = Column(Uuid(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    __table_args__ = (UniqueConstraint("period_id", "idempotency_key", name="uq_job_key"),)


class AuditEvent(Base):
    __tablename__ = "audit_events"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uid)
    source_id = Column(Uuid(as_uuid=True), ForeignKey("data_sources.id"), nullable=True)
    actor_id = Column(Uuid(as_uuid=True), ForeignKey("users.id"), nullable=True)
    action = Column(String(100), nullable=False)
    entity_type = Column(String(60), nullable=False)
    entity_id = Column(String(60), nullable=False)
    details = Column(JSON, nullable=False)
    request_id = Column(String(70), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
