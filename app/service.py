"""Transactional financial operations. All money remains integer minor units."""
import hashlib
import json
import os
import uuid
from datetime import timedelta, datetime, timezone
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError

from app.models import (
    AuditEvent, ImportBatch, MatchGroup, MatchMember, MatchRule, Period, PeriodSnapshot,
    ReconcileIssue, Record, ReconcileJob, Source, User, utcnow,
)
from app.security import ensure_source, require_role


def fail(status, message):
    raise HTTPException(status, message)


def audit(db, actor, action, entity, entity_id, details=None, source_id=None):
    from app.request_context import request_id
    db.add(AuditEvent(
        actor_id=actor.id if actor else None, action=action,
        entity_type=entity, entity_id=str(entity_id),
        details=details or {}, source_id=source_id,
        request_id=request_id.get(),
    ))


def ensure_period_source_access(db, actor, period_id):
    """A controller must be authorized for EVERY source touched by a period.

    Closed periods can include source records and rejected imports, not only
    committed batches. Job rule snapshots may reference a source even when
    no financial rows were imported. Avoid checking only the visible report.
    """
    require_role(actor, "controller")
    sources = set()
    for model in (Record, ImportBatch, MatchGroup, ReconcileIssue):
        sources.update(db.scalars(select(model.source_id).where(
            model.period_id == period_id).distinct()))
    for frozen in db.scalars(select(ReconcileJob.rules_snapshot).where(
        ReconcileJob.period_id == period_id)):
        for rule in frozen:
            sources.add(UUID(rule["source_id"]))
    for source_id in sorted(sources):
        ensure_source(db, actor, source_id)


def locked_period(db, period_id):
    period = db.scalar(select(Period).where(Period.id == period_id).with_for_update())
    if not period:
        fail(404, "Period not found")
    if period.status != "open":
        fail(409, "Period is closed; financial records are read-only")
    return period


def current_rules(db, source_id):
    rules = db.scalars(select(MatchRule).where(
        MatchRule.source_id == source_id, MatchRule.current.is_(True)
    ).order_by(MatchRule.priority.desc(), MatchRule.rule_key)).all()
    return [{
        "id": str(r.id), "key": r.rule_key, "version": r.version,
        "priority": r.priority, "window": r.day_window,
        "tolerance": r.tolerance_minor, "require_reference": r.require_reference,
        "source_id": str(r.source_id),
    } for r in rules]


def submit_job(db, period_id, key, actor):
    require_role(actor, "operator", "controller")
    period = locked_period(db, period_id)
    if not 8 <= len(key) <= 120:
        fail(400, "Idempotency-Key must have 8..120 characters")
    existing = db.scalar(select(ReconcileJob).where(
        ReconcileJob.period_id == period.id, ReconcileJob.idempotency_key == key
    ))
    if existing:
        return existing, False
    sources = db.scalars(select(Source)).all()
    rules = []
    for source in sources:
        try:
            ensure_source(db, actor, source.id)
        except HTTPException:
            continue
        rules.extend(current_rules(db, source.id))
    if not rules:
        fail(409, "No applicable current matching rules")
    job = ReconcileJob(period_id=period.id, idempotency_key=key,
                       rules_snapshot=rules, created_by=actor.id)
    db.add(job)
    db.flush()
    audit(db, actor, "job.queued", "ReconcileJob", job.id,
          {"rules": rules, "key": key})
    return job, True


def _available(db, record_id):
    return not db.scalar(select(MatchMember.id).where(
        MatchMember.record_id == record_id, MatchMember.active.is_(True)
    ).limit(1))


def _existing_proposal(db, record_id):
    return db.scalar(
        select(MatchGroup.id).join(MatchMember, MatchMember.group_id == MatchGroup.id)
        .where(MatchMember.record_id == record_id, MatchGroup.state == "proposed").limit(1)
    ) is not None


def _issue(db, record, issue_type, candidates):
    existing = db.scalar(select(ReconcileIssue).where(
        ReconcileIssue.period_id == record.period_id,
        ReconcileIssue.record_id == record.id,
        ReconcileIssue.issue_type == issue_type,
    ))
    if existing:
        return
    db.add(ReconcileIssue(
        period_id=record.period_id, source_id=record.source_id,
        record_id=record.id, issue_type=issue_type, candidates=candidates[:10],
    ))


def _resolve_issues(db, ids):
    issues = db.scalars(select(ReconcileIssue).where(
        ReconcileIssue.record_id.in_(ids), ReconcileIssue.state == "open"
    )).all()
    for issue in issues:
        issue.state = "resolved"


def _make_group(db, period, source_id, rule, external, internal,
                actor=None, comment=None, manual=False, request_key=None):
    if len(external) + len(internal) > 11 or min(len(external), len(internal)) != 1:
        fail(400, "Only 1:1, n:1 and 1:n groups with at most 10 on the many side")
    if len(external) > 1 and len(internal) > 1:
        fail(400, "Many-to-many groups are not supported")
    records = external + internal
    if len(set(r.id for r in records)) != len(records):
        fail(400, "Duplicate record in match group")
    if any(r.period_id != period.id or r.source_id != source_id for r in records):
        fail(409, "Records must belong to the same source and open period")
    if len({r.account for r in records}) != 1 or len({r.currency for r in records}) != 1:
        fail(409, "Different accounts or currencies cannot be matched")
    dates = [r.booked_date for r in records]
    if (max(dates) - min(dates)).days > rule["window"]:
        fail(409, "Group dates exceed the selected rule window")
    ext_total = sum(r.amount_minor for r in external)
    int_total = sum(r.amount_minor for r in internal)
    delta = ext_total - int_total
    if abs(delta) > rule["tolerance"]:
        fail(409, "Group difference exceeds rule tolerance")
    group = MatchGroup(
        period_id=period.id, source_id=source_id, rule_id=UUID(rule["id"]),
        rule_version=rule["version"], manual=manual,
        proposed_by=actor.id if actor else None, comment=comment,
        explanation={
            "rule_key": rule["key"], "rule_version": rule["version"],
            "priority": rule["priority"], "tolerance_minor": rule["tolerance"],
            "day_window": rule["window"], "require_reference": rule["require_reference"],
            "external_record_ids": [str(r.id) for r in external],
            "internal_record_ids": [str(r.id) for r in internal],
            "features": {"account": records[0].account, "currency": records[0].currency,
                         "references": sorted(set(r.reference for r in records)),
                         "date_range": [min(r.booked_date for r in records).isoformat(),
                                        max(r.booked_date for r in records).isoformat()]},
        },
        external_minor=ext_total, internal_minor=int_total, delta_minor=delta,
        request_key=request_key,
    )
    db.add(group)
    db.flush()
    for row in records:
        db.add(MatchMember(group_id=group.id, record_id=row.id, active=False))
    db.flush()
    threshold = int(os.getenv("RISK_THRESHOLD_MINOR", "10000000"))
    high_risk = max(abs(ext_total), abs(int_total)) > threshold
    if not high_risk:
        _activate_group(db, group, records, actor)
    audit(db, actor, "match.proposed" if high_risk else "match.confirmed",
          "MatchGroup", group.id,
          {"manual": manual, "delta_minor": delta, "rule_version": rule["version"],
           "high_risk": high_risk, "comment": comment}, source_id)
    return group


def _activate_group(db, group, records, actor):
    for record in sorted(records, key=lambda r: str(r.id)):
        db.scalar(select(Record).where(Record.id == record.id).with_for_update())
    reserved = db.scalars(select(MatchMember.record_id).where(
        MatchMember.record_id.in_([r.id for r in records]),
        MatchMember.active.is_(True),
    )).all()
    if reserved:
        fail(409, "A record already belongs to an approved group")
    members = db.scalars(select(MatchMember).where(
        MatchMember.group_id == group.id).with_for_update()).all()
    if len(members) != len(records):
        fail(409, "Match group membership inconsistency")
    for member in members:
        member.active = True
    group.state = "approved"
    group.approved_by = actor.id if actor else None
    group.approved_at = utcnow()
    group.version += 1
    _resolve_issues(db, [r.id for r in records])
    db.flush()


def create_manual_group(db, period_id, source_id, external_ids, internal_ids,
                        rule_id, actor, comment, request_key):
    require_role(actor, "operator")
    ensure_source(db, actor, source_id)
    period = locked_period(db, period_id)
    if not comment or len(comment.strip()) < 5:
        fail(400, "Manual matching requires a meaningful comment")
    if not request_key or len(request_key) < 8 or len(request_key) > 120:
        fail(400, "Idempotency-Key must have 8..120 characters")
    existing = db.scalar(select(MatchGroup).where(
        MatchGroup.period_id == period.id, MatchGroup.request_key == request_key
    ))
    if existing:
        if existing.proposed_by != actor.id:
            fail(409, "Idempotency key belongs to another user")
        return existing
    ids = [UUID(str(x)) for x in external_ids + internal_ids]
    if not ids or len(set(ids)) != len(ids):
        fail(400, "Invalid or duplicate identifiers")
    records = db.scalars(select(Record).where(Record.id.in_(ids)).with_for_update()).all()
    if len(records) != len(ids):
        fail(404, "One or more records do not exist")
    by_id = {r.id: r for r in records}
    external = [by_id[UUID(str(x))] for x in external_ids]
    internal = [by_id[UUID(str(x))] for x in internal_ids]
    if any(r.kind != "external" for r in external) or any(r.kind != "internal" for r in internal):
        fail(400, "Wrong record kind")
    if any(not _available(db, r.id) for r in records):
        fail(409, "One or more records already approved")
    rule = db.get(MatchRule, rule_id)
    if not rule or rule.source_id != source_id:
        fail(404, "Matching rule not found for source")
    frozen = {
        "id": str(rule.id), "key": rule.rule_key, "version": rule.version,
        "priority": rule.priority, "window": rule.day_window,
        "tolerance": rule.tolerance_minor, "require_reference": rule.require_reference,
    }
    return _make_group(db, period, source_id, frozen, external, internal,
                       actor, comment.strip(), True, request_key)


def approve_group(db, group_id, actor, expected_version):
    require_role(actor, "controller")
    group = db.scalar(select(MatchGroup).where(MatchGroup.id == group_id).with_for_update())
    if not group:
        fail(404, "Match group not found")
    ensure_source(db, actor, group.source_id)
    locked_period(db, group.period_id)
    if group.state != "proposed" or group.version != expected_version:
        fail(409, "Group state or version conflict")
    if group.proposed_by == actor.id:
        fail(403, "Self-approval is prohibited")
    record_ids = db.scalars(select(MatchMember.record_id).where(
        MatchMember.group_id == group.id)).all()
    rows = db.scalars(select(Record).where(Record.id.in_(record_ids))).all()
    _activate_group(db, group, rows, actor)
    audit(db, actor, "match.approved", "MatchGroup", group.id,
          {"proposed_by": str(group.proposed_by) if group.proposed_by else None,
           "version": group.version}, group.source_id)
    return group


def cancel_group(db, group_id, actor, reason, expected_version):
    require_role(actor, "controller")
    group = db.scalar(select(MatchGroup).where(MatchGroup.id == group_id).with_for_update())
    if not group:
        fail(404, "Match group not found")
    ensure_source(db, actor, group.source_id)
    locked_period(db, group.period_id)
    if group.state != "approved" or group.version != expected_version:
        fail(409, "Group state or version conflict")
    if not reason or len(reason.strip()) < 10:
        fail(400, "Cancellation requires a reason (at least 10 characters)")
    members = db.scalars(select(MatchMember).where(
        MatchMember.group_id == group.id).with_for_update()).all()
    for m in members:
        m.active = False
    group.state = "cancelled"
    group.version += 1
    audit(db, actor, "match.cancelled", "MatchGroup", group.id,
          {"reason": reason, "version": group.version}, group.source_id)
    return group



def _batch_insert(db, model, values, size=500):
    """Bound SQL statement size while preserving one all-or-nothing transaction."""
    from sqlalchemy import insert
    for offset in range(0, len(values), size):
        db.execute(insert(model), values[offset:offset + size])


def _build_rule_indexes(rows, rules):
    """Date or amount ordered candidate indexes, built once per source and side.

    Indexes are created only for the four lookup modes actually used by the
    frozen rules. We keep compact SQL rows, not ORM Record objects or raw files.
    """
    from collections import defaultdict

    modes = {(bool(rule["require_reference"]), rule["tolerance"] == 0)
             for rule in rules}
    groups = {mode: defaultdict(list) for mode in modes}
    for row in rows:
        for ref_required, exact in modes:
            key = (row.account, row.currency)
            if ref_required:
                key += (row.reference,)
            if exact:
                key += (row.amount_minor,)
            groups[ref_required, exact][key].append(row)

    indexes = {}
    for mode, buckets in groups.items():
        exact = mode[1]
        prepared = {}
        for key, items in buckets.items():
            if exact:
                items.sort(key=lambda row: (row.booked_date, row.id))
                prepared[key] = ([row.booked_date for row in items], items)
            else:
                items.sort(key=lambda row: (row.amount_minor, row.booked_date, row.id))
                prepared[key] = ([row.amount_minor for row in items], items)
        indexes[mode] = prepared
    return indexes


def _rule_candidates(indexes, record, rule, forbidden, maximum=11):
    """Bounded 1:1 candidate search across exact/tolerant and ref/broad rules."""
    from bisect import bisect_left, bisect_right

    if maximum < 1:
        return []
    ref_required = bool(rule["require_reference"])
    tolerance = int(rule["tolerance"])
    exact = tolerance == 0
    key = (record.account, record.currency)
    if ref_required:
        key += (record.reference,)
    if exact:
        key += (record.amount_minor,)
    group = indexes.get((ref_required, exact), {}).get(key)
    if not group:
        return []
    positions, items = group
    if exact:
        lo = bisect_left(
            positions, record.booked_date - timedelta(days=rule["window"]))
        hi = bisect_right(
            positions, record.booked_date + timedelta(days=rule["window"]))
    else:
        lo = bisect_left(positions, record.amount_minor - tolerance)
        hi = bisect_right(positions, record.amount_minor + tolerance)
    found = []
    for index in range(lo, hi):
        other = items[index]
        if other.id in forbidden:
            continue
        if not exact and abs((other.booked_date - record.booked_date).days) > rule["window"]:
            continue
        found.append(other)
        if len(found) >= maximum:
            break
    return found


def _reconcile_indexed_rules(db, period, source_id, rules, submitter_id,
                             active, pending):
    """Batch reconcile all priorities and rule types in one source transaction.

    At the first priority with any candidates, all rules at that priority
    contribute to ambiguity. A pair is accepted only if forward and reverse
    lookups are both unique. No per-record SQL candidate queries are needed.
    """
    from itertools import groupby
    from sqlalchemy import update

    submitter = db.get(User, submitter_id)
    if submitter is None or not submitter.enabled:
        fail(409, "Reconciliation submitter no longer exists")

    rules = sorted(rules, key=lambda r: (-r["priority"], r["key"]))
    priority_groups = [
        list(group) for _, group in groupby(rules, lambda r: r["priority"])
    ]
    rows = db.execute(select(
        Record.id, Record.kind, Record.account, Record.currency,
        Record.reference, Record.amount_minor, Record.booked_date,
    ).where(
        Record.period_id == period.id,
        Record.source_id == source_id,
    ).order_by(Record.id)).all()
    external, internal = [], []
    any_internal = set()
    for row in rows:
        if row.kind == "internal":
            internal.append(row)
            any_internal.add((row.reference, row.account))
        elif row.kind == "external":
            external.append(row)
    del rows

    internal_index = _build_rule_indexes(internal, rules)
    external_index = _build_rule_indexes(external, rules)
    del internal

    existing_issues = set(db.execute(
        select(ReconcileIssue.record_id, ReconcileIssue.issue_type).where(
            ReconcileIssue.period_id == period.id,
            ReconcileIssue.source_id == source_id,
        )
    ).all())
    new_issues = {}
    groups, members, events, approved_ids = [], [], [], []
    threshold = int(os.getenv("RISK_THRESHOLD_MINOR", "10000000"))
    now = utcnow()
    stats = {"matched": 0, "proposed": 0, "ambiguous": 0, "unmatched": 0}

    def flush_issues():
        if new_issues:
            _batch_insert(db, ReconcileIssue, list(new_issues.values()))
            existing_issues.update(new_issues.keys())
            new_issues.clear()

    def flush_groups():
        if not groups:
            return
        _batch_insert(db, MatchGroup, groups)
        _batch_insert(db, MatchMember, members)
        for offset in range(0, len(approved_ids), 500):
            db.execute(update(ReconcileIssue).where(
                ReconcileIssue.period_id == period.id,
                ReconcileIssue.source_id == source_id,
                ReconcileIssue.record_id.in_(approved_ids[offset:offset + 500]),
                ReconcileIssue.state == "open",
            ).values(state="resolved"))
        _batch_insert(db, AuditEvent, events)
        groups.clear()
        members.clear()
        events.clear()
        approved_ids.clear()

    def record_issue(record_id, issue_type, candidates):
        key = (record_id, issue_type)
        if key in existing_issues or key in new_issues:
            return
        new_issues[key] = {
            "id": uuid.uuid4(), "period_id": period.id, "source_id": source_id,
            "record_id": record_id, "issue_type": issue_type,
            "candidates": [str(item) for item in candidates[:10]],
            "state": "open",
        }
        if len(new_issues) >= 500:
            flush_issues()

    def collect(indexes, row, priority_rules, forbidden):
        """Deduplicate candidate records across equal-priority rules."""
        found = {}
        for rule in priority_rules:
            for candidate in _rule_candidates(
                    indexes, row, rule, forbidden, maximum=11):
                found.setdefault(candidate.id, (candidate, rule))
                if len(found) >= 11:
                    break
            if len(found) >= 11:
                break
        return found

    forbidden = active | pending
    for ext in external:
        if ext.id in forbidden:
            continue
        selected = None
        ambiguous = False
        for priority_rules in priority_groups:
            candidates = collect(internal_index, ext, priority_rules, forbidden)
            if not candidates:
                continue
            if len(candidates) > 1:
                record_issue(ext.id, "ambiguous", list(candidates))
                stats["ambiguous"] += 1
                ambiguous = True
                break
            internal_row, rule = next(iter(candidates.values()))

            reverse = collect(external_index, internal_row, priority_rules, forbidden)
            if len(reverse) > 1:
                # Reverse candidates are intentionally capped. The current
                # external record might fall beyond that cap: it still needs
                # its own ambiguity issue rather than disappearing silently.
                reverse_ids = list(reverse)
                record_issue(ext.id, "ambiguous", [ext.id] + reverse_ids)
                for candidate in reverse.values():
                    record_issue(candidate[0].id, "ambiguous", reverse_ids)
                stats["ambiguous"] += 1
                ambiguous = True
                break
            if len(reverse) != 1 or ext.id not in reverse:
                raise RuntimeError("Multi-rule matcher detected asymmetric candidate set")
            selected = (internal_row, rule)
            break

        if ambiguous:
            continue
        if selected is None:
            record_issue(
                ext.id,
                "amount_mismatch" if (ext.reference, ext.account) in any_internal
                else "unmatched", [],
            )
            stats["unmatched"] += 1
            continue

        internal_row, rule = selected
        if ext.account != internal_row.account or ext.currency != internal_row.currency:
            raise RuntimeError("Cross-account/currency match rejected")
        if rule["require_reference"] and ext.reference != internal_row.reference:
            raise RuntimeError("Reference-aware match rejected")
        if abs((ext.booked_date - internal_row.booked_date).days) > rule["window"]:
            raise RuntimeError("Matched dates exceed rule window")
        delta = ext.amount_minor - internal_row.amount_minor
        if abs(delta) > rule["tolerance"]:
            raise RuntimeError("Matched amount exceeds rule tolerance")

        high_risk = max(abs(ext.amount_minor), abs(internal_row.amount_minor)) > threshold
        group_id = uuid.uuid4()
        explanation = {
            "rule_key": rule["key"], "rule_version": rule["version"],
            "priority": rule["priority"], "tolerance_minor": rule["tolerance"],
            "day_window": rule["window"],
            "require_reference": rule["require_reference"],
            "external_record_ids": [str(ext.id)],
            "internal_record_ids": [str(internal_row.id)],
            "features": {
                "account": ext.account, "currency": ext.currency,
                "references": sorted({ext.reference, internal_row.reference}),
                "date_range": [
                    min(ext.booked_date, internal_row.booked_date).isoformat(),
                    max(ext.booked_date, internal_row.booked_date).isoformat(),
                ],
            },
        }
        groups.append({
            "id": group_id, "period_id": period.id, "source_id": source_id,
            "rule_id": UUID(rule["id"]), "rule_version": rule["version"],
            "state": "proposed" if high_risk else "approved", "manual": False,
            "proposed_by": submitter_id,
            "approved_by": None if high_risk else submitter_id,
            "approved_at": None if high_risk else now,
            "version": 1 if high_risk else 2, "explanation": explanation,
            "external_minor": ext.amount_minor, "internal_minor": internal_row.amount_minor,
            "delta_minor": delta,
        })
        members.extend([
            {"group_id": group_id, "record_id": ext.id, "active": not high_risk},
            {"group_id": group_id, "record_id": internal_row.id, "active": not high_risk},
        ])
        events.append({
            "id": uuid.uuid4(), "source_id": source_id, "actor_id": submitter_id,
            "action": "match.proposed" if high_risk else "match.confirmed",
            "entity_type": "MatchGroup", "entity_id": str(group_id),
            "details": {
                "manual": False, "delta_minor": delta,
                "rule_version": rule["version"],
                "high_risk": high_risk, "comment": None,
            },
        })
        forbidden.update((ext.id, internal_row.id))
        if high_risk:
            pending.update((ext.id, internal_row.id))
            stats["proposed"] += 1
        else:
            active.update((ext.id, internal_row.id))
            approved_ids.extend((ext.id, internal_row.id))
            for record_id in (ext.id, internal_row.id):
                for category in ("ambiguous", "unmatched", "amount_mismatch",
                                 "wrong_currency", "duplicate"):
                    staged = new_issues.get((record_id, category))
                    if staged is not None:
                        staged["state"] = "resolved"
            stats["matched"] += 1

        if len(groups) >= 500:
            flush_issues()
            flush_groups()

    flush_issues()
    flush_groups()
    return stats


def reconcile(db, job):
    """Idempotent indexed matching at the highest applicable rule priority."""
    period = locked_period(db, job.period_id)
    active = set(db.scalars(select(MatchMember.record_id).join(
        Record, Record.id == MatchMember.record_id).where(
            Record.period_id == period.id, MatchMember.active.is_(True))).all())
    pending = set(db.scalars(select(MatchMember.record_id).join(
        MatchGroup, MatchMember.group_id == MatchGroup.id).where(
            MatchGroup.state == "proposed",
            MatchGroup.period_id == period.id)).all())

    rules_by_source = {}
    for rule in job.rules_snapshot:
        rules_by_source.setdefault(rule["source_id"], []).append(rule)
    stats = {"matched": 0, "proposed": 0, "ambiguous": 0, "unmatched": 0}
    for source_id, rules in rules_by_source.items():
        result = _reconcile_indexed_rules(
            db, period, UUID(source_id), rules, job.created_by, active, pending)
        for metric, value in result.items():
            stats[metric] += value
    audit(db, None, "job.completed", "ReconcileJob", job.id, stats)
    return stats


def build_report(db, period):
    rows = db.execute(select(
        Record.source_id, Record.account, Record.currency, Record.kind,
        func.count(Record.id), func.sum(Record.amount_minor)
    ).where(Record.period_id == period.id).group_by(
        Record.source_id, Record.account, Record.currency, Record.kind
    )).all()
    summary = {}
    for source, account, currency, kind, count, amount in rows:
        key = (str(source), account, currency)
        if key not in summary:
            summary[key] = {
                "source_id": str(source), "account": account, "currency": currency,
                "external_count": 0, "external_minor": 0,
                "internal_count": 0, "internal_minor": 0,
                "confirmed_external_minor": 0, "confirmed_count": 0, "open_issues": 0,
            }
        summary[key][f"{kind}_count"] = int(count)
        summary[key][f"{kind}_minor"] = int(amount or 0)
    groups = db.execute(select(
        MatchGroup.source_id, func.sum(MatchGroup.external_minor),
        func.count(MatchGroup.id)
    ).where(
        MatchGroup.period_id == period.id,
        MatchGroup.state == "approved"
    ).group_by(MatchGroup.source_id)).all()
    for source, amount, count in groups:
        for key, val in summary.items():
            if key[0] == str(source):
                val["confirmed_external_minor"] += int(amount or 0)
                val["confirmed_count"] += int(count)
                break
    issues = db.execute(select(
        ReconcileIssue.source_id, func.count(ReconcileIssue.id)
    ).where(
        ReconcileIssue.period_id == period.id, ReconcileIssue.state == "open"
    ).group_by(ReconcileIssue.source_id)).all()
    for source, count in issues:
        for key, val in summary.items():
            if key[0] == str(source):
                val["open_issues"] += int(count)
                break
    for val in summary.values():
        val["difference_minor"] = val["external_minor"] - val["confirmed_external_minor"]
    return {
        "period_id": str(period.id), "period_name": period.name,
        "by_source": sorted(summary.values(), key=lambda r: (r["source_id"], r["account"], r["currency"])),
    }


def close_period(db, period_id, actor):
    require_role(actor, "controller")
    period = locked_period(db, period_id)
    if db.scalar(select(ReconcileJob.id).where(
        ReconcileJob.period_id == period.id,
        ReconcileJob.state.in_(("queued", "running"))).limit(1)):
        fail(409, "Cannot close period with unfinished reconciliation jobs")
    if db.scalar(select(MatchGroup.id).where(
        MatchGroup.period_id == period.id, MatchGroup.state == "proposed").limit(1)):
        fail(409, "Cannot close period with unapproved match groups")
    batches = db.scalars(select(ImportBatch).where(ImportBatch.period_id == period.id)
                        .order_by(ImportBatch.id)).all()
    rules = db.scalars(select(MatchRule).order_by(
        MatchRule.source_id, MatchRule.rule_key, MatchRule.version)).all()
    report = build_report(db, period)
    snapshot = {
        "schema_version": 1, "period_id": str(period.id),
        "batch_ids": [str(x.id) for x in batches],
        "batch_hashes": [x.file_sha256 for x in batches],
        "rules": [
            {"id": str(r.id), "source_id": str(r.source_id), "key": r.rule_key,
             "version": r.version, "priority": r.priority,
             "tolerance_minor": r.tolerance_minor, "day_window": r.day_window}
            for r in rules
        ], "report": report,
    }
    checksum = hashlib.sha256(json.dumps(
        snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    period.snapshot = snapshot
    period.snapshot_sha256 = checksum
    period.status = "closed"
    period.closed_by = actor.id
    period.closed_at = utcnow()
    period.version += 1
    db.add(PeriodSnapshot(
        period_id=period.id, sequence=period.version,
        content=snapshot, sha256=checksum, closed_by=actor.id,
    ))
    audit(db, actor, "period.closed", "Period", period.id,
          {"sha256": checksum, "batch_count": len(batches)})
    return period


def reopen_period(db, period_id, actor, reason):
    require_role(actor, "controller")
    if not reason or len(reason.strip()) < 10:
        fail(400, "Reopening requires a reason (at least 10 characters)")
    period = db.scalar(select(Period).where(Period.id == period_id).with_for_update())
    if not period:
        fail(404, "Period not found")
    if period.status != "closed":
        fail(409, "Period already open")
    old_hash = period.snapshot_sha256
    period.status = "open"
    period.version += 1
    audit(db, actor, "period.reopened", "Period", period.id,
          {"reason": reason.strip(), "previous_snapshot_sha256": old_hash})
    return period
