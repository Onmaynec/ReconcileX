"""Durable database-backed worker; DB transaction rolls back crash-side effects."""
import logging
import os
import time
from datetime import timedelta
from uuid import UUID

from sqlalchemy import or_, select

from app.db import SessionLocal
from app.models import ReconcileJob, SourceGrant, User, utcnow
from app.service import reconcile

log = logging.getLogger("reconcilex.worker")


def run_once():
    job_id = None
    with SessionLocal.begin() as db:
        candidate = db.scalar(
            select(ReconcileJob).where(or_(
                ReconcileJob.state == "queued",
                (ReconcileJob.state == "running") & (ReconcileJob.lease_until < utcnow()),
            )).order_by(ReconcileJob.created_at).with_for_update(skip_locked=True).limit(1)
        )
        if candidate:
            candidate.state = "running"
            candidate.attempts += 1
            candidate.lease_until = utcnow() + timedelta(minutes=10)
            job_id = candidate.id
    if not job_id:
        return False
    try:
        with SessionLocal.begin() as db:
            job = db.scalar(select(ReconcileJob).where(
                ReconcileJob.id == job_id).with_for_update())
            if not job or job.state != "running":
                return True
            # Lock the submitter and grants until the reconciliation transaction
            # commits. A concurrent disable/revoke waits instead of allowing
            # a financial commit after access has been revoked.
            owner = db.scalar(select(User).where(
                User.id == job.created_by).with_for_update())
            if not owner or not owner.enabled or owner.role not in {"operator", "controller"}:
                raise PermissionError("Submitting account disabled or no longer authorized")
            for source_id in sorted({UUID(rule["source_id"]) for rule in job.rules_snapshot}):
                grant = db.scalar(select(SourceGrant).where(
                    SourceGrant.user_id == job.created_by,
                    SourceGrant.source_id == source_id).with_for_update())
                if not grant:
                    raise PermissionError("Source access revoked before job execution")
            result = reconcile(db, job)
            job.state = "done"
            job.lease_until = None
            job.error = None
            log.info("reconciliation complete %s: %s", job_id, result)
    except Exception as exc:
        log.exception("job failed: %s", job_id)
        with SessionLocal.begin() as db:
            job = db.scalar(select(ReconcileJob).where(
                ReconcileJob.id == job_id).with_for_update())
            if job and job.state == "running":
                job.state = "failed"
                job.lease_until = None
                job.error = str(exc)[:1000]
    return True


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    pause = float(os.getenv("WORKER_POLL_SECONDS", "2"))
    while True:
        if not run_once():
            time.sleep(pause)


if __name__ == "__main__":
    main()
