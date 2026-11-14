"""Reproducible PV-FIN-03 synthetic preview/import performance protocol.

Parser-only mode does not connect to the database.
--db-load writes synthetic data; restricted to *_test database by default.
This is not an SLO claim until run on a named representative machine.
"""
import argparse
import hashlib
import json
import os
import platform
import resource
import statistics
import sys
import time
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace


def csv_bytes(kind, rows, month=1):
    prefix = "I" if kind == "internal" else "E"
    content = ["reference,date,amount\n"]
    for i in range(rows):
        content.append(f"{prefix}-{i:08d},2099-{month:02d}-{(i % 28) + 1:02d},{100 + i % 199}.{i % 100:02d}\n")
    return "".join(content).encode("utf-8")


def percentile(values, percentage):
    values = sorted(values)
    position = max(0, (len(values) * percentage + 99) // 100 - 1)
    return round(values[position], 3)


def memory_mib():
    # ru_maxrss is KiB on Linux; record platform for interpretation.
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)


def host_details():
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        pages = os.sysconf("SC_PHYS_PAGES")
        ram_gib = round(page_size * pages / 1024**3, 2)
    except (ValueError, OSError, AttributeError):
        ram_gib = None
    return {
        "platform": platform.platform(), "python": sys.version.split()[0],
        "cpu_count": os.cpu_count(), "ram_gib": ram_gib,
    }


def test_parser(data, kind, trials):
    from app.importer import inspect_file
    profile = SimpleNamespace(
        encoding="utf-8", delimiter=",", date_format="%Y-%m-%d",
        decimal_separator=".", mapping={"reference": "reference",
                                        "date": "date", "amount": "amount"},
    )
    source = SimpleNamespace(currency="RUB", account="BENCH-TEST")
    times = []
    last = None
    for _ in range(trials):
        start = time.perf_counter()
        last = inspect_file(data, f"{kind}.csv", profile, source, kind,
                            date(2099, 1, 1), date(2099, 1, 31))
        times.append((time.perf_counter() - start) * 1000)
        if last["errors"]:
            raise RuntimeError(f"Unexpected synthetic parser failures: {last['errors'][:2]}")
    return last, {
        "kind": kind, "input_bytes": len(data), "rows": last["valid_rows"],
        "sum_minor": last["total_minor"],
        "p50_ms": percentile(times, 50), "p95_ms": percentile(times, 95),
        "times_ms": [round(v, 3) for v in times],
    }


def load_db(parts):
    """Bulk insert in short atomic transactions; no automatic production writes."""
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("--db-load requires DATABASE_URL")
    if "_test" not in url.rsplit("/", 1)[-1] and os.environ.get("ALLOW_BENCHMARK_WRITES") != "1":
        raise RuntimeError("Refusing writes to non-test DB without ALLOW_BENCHMARK_WRITES=1")
    from sqlalchemy import insert, select, func
    from app.db import SessionLocal
    from app.models import ImportBatch, ImportProfile, Period, Record, Source, User
    from app.storage import put_original

    started = time.perf_counter()
    token = uuid.uuid4().hex[:12]
    source_name = "benchmark-" + token
    with SessionLocal.begin() as db:
        user = db.scalar(select(User).where(User.username == "operator"))
        if user is None:
            raise RuntimeError("Seed demo operator first: python -m app.seed")
        source = Source(name=source_name, account="BENCH-TEST", currency="RUB")
        period = Period(name=source_name, start_date=date(2099, 1, 1),
                        end_date=date(2099, 1, 31))
        db.add_all((source, period))
        db.flush()
        profile = ImportProfile(
            source_id=source.id, name="benchmark-csv", version=1,
            encoding="utf-8", delimiter=",", date_format="%Y-%m-%d",
            mapping={"reference": "reference", "date": "date", "amount": "amount"})
        db.add(profile)
        db.flush()
        src_id, per_id, pro_id, user_id = source.id, period.id, profile.id, user.id

    counts = {}
    for kind, data, parsed in parts:
        t0 = time.perf_counter()
        digest = hashlib.sha256(data).hexdigest()
        key = f"originals/{src_id}/{digest}"
        put_original(key, data)
        with SessionLocal.begin() as db:
            batch = ImportBatch(
                source_id=src_id, period_id=per_id, profile_id=pro_id,
                file_sha256=digest, object_key=key,
                filename=f"benchmark_{kind}.csv", kind=kind,
                row_count=len(parsed["records"]), total_minor=parsed["total_minor"],
                created_by=user_id,
            )
            db.add(batch)
            db.flush()
            for start in range(0, len(parsed["records"]), 500):
                chunk = parsed["records"][start:start + 500]
                db.execute(insert(Record), [{
                    "source_id": src_id, "period_id": per_id, "batch_id": batch.id,
                    "kind": kind, "natural_key": r["natural_key"],
                    "reference": r["reference"], "account": r["account"],
                    "currency": r["currency"], "amount_minor": r["amount_minor"],
                    "booked_date": r["booked_date"], "counterparty": r["counterparty"],
                    "row_number": r["row_number"], "raw_text": r["raw_text"],
                    "normalized": r["normalized"],
                } for r in chunk])
        counts[kind] = {
            "rows": len(parsed["records"]),
            "load_ms": round((time.perf_counter() - t0) * 1000, 3),
        }
    with SessionLocal() as db:
        actual = db.execute(select(Record.kind, func.count(Record.id),
                                   func.sum(Record.amount_minor)).where(
            Record.period_id == per_id).group_by(Record.kind)).all()
        for kind, count, amount in actual:
            expected = next(p for p in parts if p[0] == kind)[2]
            if count != len(expected["records"]) or amount != expected["total_minor"]:
                raise RuntimeError("Financial invariant failed after bulk import")
    return {
        "period": str(per_id), "source": str(src_id), "duration_ms": round(
            (time.perf_counter() - started) * 1000, 3), "batches": counts,
        "verified": True,
    }


def main():
    parser = argparse.ArgumentParser(description="Synthetic performance protocol")
    parser.add_argument("--external", type=int, default=80000)
    parser.add_argument("--internal", type=int, default=100000)
    parser.add_argument("--preview", type=int, default=20000)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--db-load", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--preview-limit-s", type=float, default=120)
    args = parser.parse_args()
    if min(args.external, args.internal, args.preview, args.trials) < 1:
        parser.error("Rows and trial count must be positive")
    if max(args.external, args.internal, args.preview) > 250000:
        parser.error("Individual files limited to 250000 records")

    start = time.perf_counter()
    parts = []
    measurements = []
    for kind, count in (("external", args.external), ("internal", args.internal),
                        ("preview", args.preview)):
        if kind == "preview":
            parsed, metric = test_parser(csv_bytes("external", count), "external", args.trials)
        else:
            data = csv_bytes(kind, count)
            parsed, metric = test_parser(data, kind, args.trials)
            parts.append((kind, data, parsed))
        measurements.append(metric)
        if kind == "preview" and metric["p95_ms"] > args.preview_limit_s * 1000:
            raise RuntimeError("Preview p95 exceeded the configured threshold")

    result = {
        "schema": 1, "scenario": "PV-FIN-03-synthetic",
        "host": host_details(), "metrics": measurements,
        "peak_process_mib": memory_mib(),
        "total_ms": round((time.perf_counter() - start) * 1000, 3),
        "data_kind": "synthetic",
    }
    if args.db_load:
        result["db_import"] = load_db(parts)
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
