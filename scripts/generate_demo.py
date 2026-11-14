"""Deterministic six-format synthetic bank/reconciliation fixture generator."""
import argparse
import csv
import json
import os
import re
import zipfile
from datetime import date, datetime
from pathlib import Path

from openpyxl import Workbook

FORMATS = [
    {"file": "01_external_rub_aug.csv", "source": "demo-rouble", "kind": "external",
     "month": 8, "currency": "RUB", "encoding": "utf-8", "delimiter": ",",
     "date_format": "%Y-%m-%d", "decimal_separator": ".",
     "mapping": {"reference": "reference", "date": "date", "amount": "amount"}},
    {"file": "02_internal_rub_aug.csv", "source": "demo-rouble", "kind": "internal",
     "month": 8, "currency": "RUB", "encoding": "cp1251", "delimiter": ";",
     "date_format": "%d.%m.%Y", "decimal_separator": ",",
     "mapping": {"reference": "Ссылка", "date": "Дата", "amount": "Сумма"}},
    {"file": "03_external_eur_sep.csv", "source": "demo-euro", "kind": "external",
     "month": 9, "currency": "EUR", "encoding": "utf-8", "delimiter": "\t",
     "date_format": "%Y-%m-%d", "decimal_separator": ".",
     "mapping": {"reference": "reference", "date": "date", "amount": "amount"}},
    {"file": "04_internal_eur_sep.csv", "source": "demo-euro", "kind": "internal",
     "month": 9, "currency": "EUR", "encoding": "utf-8", "delimiter": "|",
     "date_format": "%Y-%m-%d", "decimal_separator": ".",
     "mapping": {"reference": "reference", "date": "date", "amount": "amount"}},
    {"file": "05_external_rub_oct.xlsx", "source": "demo-rouble", "kind": "external",
     "month": 10, "currency": "RUB", "encoding": "xlsx", "delimiter": None,
     "date_format": "%Y-%m-%d", "decimal_separator": ".",
     "mapping": {"reference": "Ref", "date": "Booking", "amount": "Amount"}},
    {"file": "06_internal_rub_oct.xlsx", "source": "demo-rouble", "kind": "internal",
     "month": 10, "currency": "RUB", "encoding": "xlsx", "delimiter": None,
     "date_format": "%Y-%m-%d", "decimal_separator": ".",
     "mapping": {"reference": "Ref", "date": "Booking", "amount": "Amount"}},
]


def amount_string(minor, sep):
    sign = "-" if minor < 0 else ""
    absolute = abs(minor)
    return sign + f"{absolute // 100}{sep}{absolute % 100:02}"


def rows_for(spec, number):
    month = spec["month"]
    prefix = "R" if month == 8 else "E" if month == 9 else "O"
    for index in range(number):
        booked = date(2026, month, 1 + index % 26)
        if spec["kind"] == "internal" and index % 9 == 0:
            booked = date(2026, month, 2 + index % 26)
        reference = f"{prefix}-{index:07d}"
        minor = 10000 + (index % 9000)
        if month == 8 and index == 0:
            minor = 98000 if spec["kind"] == "external" else 100000
        if month == 8 and index == 1 and spec["kind"] == "internal":
            minor = -2000
            reference = "FEE-0000000"
        if index % 173 == 0 and index > 0:
            minor = -minor   # synthetic refunds/returns
        yield (reference, booked.strftime(spec["date_format"]),
               amount_string(minor, spec["decimal_separator"]))


def create_csv(path, spec, number):
    headers = list(spec["mapping"].values())
    with path.open("w", encoding=spec["encoding"], newline="") as handle:
        writer = csv.writer(handle, delimiter=spec["delimiter"])
        writer.writerow(headers)
        writer.writerows(rows_for(spec, number))


def create_xlsx(path, spec, number):
    """Canonical ZIP members and workbook timestamps make XLSX hashes reproducible."""
    book = Workbook(write_only=True)
    fixed = datetime(2026, 1, 1, 0, 0, 0)
    book.properties.created = fixed
    book.properties.modified = fixed
    sheet = book.create_sheet("Transactions")
    sheet.append(list(spec["mapping"].values()))
    for row in rows_for(spec, number):
        sheet.append(row)
    temporary = Path(str(path) + ".uncanonical")
    book.save(temporary)
    try:
        with zipfile.ZipFile(temporary, "r") as source_zip:
            members = []
            for info in source_zip.infolist():
                payload = source_zip.read(info.filename)
                if info.filename == "docProps/core.xml":
                    # openpyxl rewrites its modified property at save time.
                    payload = re.sub(
                        rb"(<dcterms:modified\b[^>]*>)[^<]*(</dcterms:modified>)",
                        rb"\g<1>2026-01-01T00:00:00Z\g<2>",
                        payload,
                    )
                members.append((info, payload))
        with zipfile.ZipFile(path, "w") as output_zip:
            for original, payload in members:
                info = zipfile.ZipInfo(original.filename, date_time=(2026, 1, 1, 0, 0, 0))
                info.compress_type = original.compress_type
                info.external_attr = original.external_attr
                info.create_system = original.create_system
                output_zip.writestr(info, payload)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=150000,
                        help="total synthetic valid rows across six formats")
    parser.add_argument("--out", default="demo-output")
    args = parser.parse_args()
    if args.rows < 6 or args.rows > 1_000_000:
        parser.error("--rows must be in [6,1000000]")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    counts = [args.rows // 6 + (1 if i < args.rows % 6 else 0) for i in range(6)]
    manifest = {"seed": "PV-FIN-03-2026", "rows": sum(counts), "files": []}
    for spec, count in zip(FORMATS, counts):
        path = out / spec["file"]
        if path.suffix == ".xlsx":
            create_xlsx(path, spec, count)
        else:
            create_csv(path, spec, count)
        manifest["files"].append({**spec, "rows": count, "bytes": path.stat().st_size})
    with (out / "invalid_rows.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["reference", "date", "amount"])
        for i in range(12):
            writer.writerow([f"BROKEN-{i}", "2026-08-12", "not-an-amount"])
    with (out / "duplicate_rows.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["reference", "date", "amount"])
        writer.writerow(["DUP-01", "2026-08-12", "10.00"])
        writer.writerow(["DUP-01", "2026-08-12", "10.00"])
    (out / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Generated {manifest['rows']} synthetic records in {out.resolve()}")


if __name__ == "__main__":
    main()
