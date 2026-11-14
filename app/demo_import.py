"""Generate and transactionally load the six demonstration bank/ledger formats.

Only synthetic data is accepted. This is a reproducible seed, not a banking connector.
"""
import argparse
import hashlib
import tempfile
from datetime import date
from pathlib import Path

from sqlalchemy import insert, select

from app.db import SessionLocal
from app.importer import inspect_file
from app.models import ImportBatch, ImportProfile, Period, Record, Source, User
from app.service import audit, locked_period
from app.storage import put_original
from scripts.generate_demo import FORMATS, create_csv, create_xlsx


def load_one(spec, count, operator):
    month = int(spec["month"])
    with tempfile.TemporaryDirectory(prefix="reconcilex-demo-") as folder:
        target = Path(folder) / spec["file"]
        if target.suffix == ".xlsx":
            create_xlsx(target, spec, count)
        else:
            create_csv(target, spec, count)
        data = target.read_bytes()

    with SessionLocal.begin() as db:
        source = db.scalar(select(Source).where(Source.name == spec["source"]))
        period = db.scalar(select(Period).where(
            Period.name == f"2026-{month:02d}"))
        if source is None or period is None:
            raise RuntimeError("Run python -m app.seed to create demo sources and periods")
        period = locked_period(db, period.id)

        name = "synthetic-" + target.stem
        profile = db.scalar(select(ImportProfile).where(
            ImportProfile.source_id == source.id, ImportProfile.name == name))
        if profile is None:
            profile = ImportProfile(
                source_id=source.id, name=name, version=1,
                encoding=spec["encoding"] if target.suffix != ".xlsx" else "utf-8",
                delimiter=spec["delimiter"] or ",",
                date_format=spec["date_format"],
                decimal_separator=spec["decimal_separator"],
                mapping=spec["mapping"],
            )
            db.add(profile)
            db.flush()

        digest = hashlib.sha256(data).hexdigest()
        previous = db.scalar(select(ImportBatch).where(
            ImportBatch.source_id == source.id, ImportBatch.file_sha256 == digest))
        if previous:
            return f"SKIP {spec['file']}: existing batch {previous.id}"
        start = period.start_date
        end = period.end_date
        result = inspect_file(data, spec["file"], profile, source, spec["kind"], start, end)
        if result["errors"] or result["valid_rows"] != count:
            raise RuntimeError(
                f"{spec['file']}: data invalid, first errors: {result['errors'][:3]}"
            )
        # All batches are disjoint by source, kind, month and synthetic reference.
        # Unique database keys still provide a final transactional safety barrier.
        object_key = f"originals/{source.id}/{result['sha256']}"
        put_original(object_key, data)
        batch = ImportBatch(
            source_id=source.id, period_id=period.id,
            profile_id=profile.id, file_sha256=result["sha256"],
            object_key=object_key, filename=spec["file"],
            kind=spec["kind"], row_count=count,
            total_minor=result["total_minor"], created_by=operator.id,
        )
        db.add(batch)
        db.flush()
        for offset in range(0, count, 500):
            payload = [{
                "source_id": source.id, "period_id": period.id,
                "batch_id": batch.id, "kind": spec["kind"],
                "natural_key": row["natural_key"],
                "reference": row["reference"], "account": row["account"],
                "currency": row["currency"], "amount_minor": row["amount_minor"],
                "booked_date": row["booked_date"],
                "counterparty": row["counterparty"],
                "row_number": row["row_number"], "raw_text": row["raw_text"],
                "normalized": row["normalized"],
            } for row in result["records"][offset:offset + 500]]
            db.execute(insert(Record), payload)
        audit(db, operator, "demo.batch_created", "ImportBatch", batch.id, {
            "file_sha256": digest, "row_count": count,
            "total_minor": result["total_minor"],
        }, source.id)
        return f"IMPORTED {spec['file']}: {count} rows, batch {batch.id}"


def main():
    parser = argparse.ArgumentParser(description="Seed synthetic financial records into PostgreSQL")
    parser.add_argument("--rows", type=int, default=150000)
    args = parser.parse_args()
    if not 6 <= args.rows <= 1_000_000:
        parser.error("rows must be between 6 and 1000000")
    counts = [args.rows // 6 + (1 if index < args.rows % 6 else 0)
              for index in range(6)]
    with SessionLocal() as db:
        user = db.scalar(select(User).where(User.username == "operator"))
        if user is None:
            raise RuntimeError("Missing demo operator; run python -m app.seed first")
        operator_id = user.id
    imported = 0
    for spec, count in zip(FORMATS, counts):
        with SessionLocal() as db:
            operator = db.get(User, operator_id)
            print(load_one(spec, count, operator))
        imported += count
    print(f"Seed target: {imported} synthetic rows. Repeated runs are file-idempotent.")


if __name__ == "__main__":
    main()
