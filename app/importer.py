import csv
import hashlib
import io
import json
import zipfile
from pathlib import Path

from openpyxl import load_workbook
from app.money import parse_minor, parse_date

MAX_BYTES = 20 * 1024 * 1024
MAX_ROWS = 250_000


def digest(data):
    return hashlib.sha256(data).hexdigest()


def _rows_csv(data, profile):
    text = data.decode(profile.encoding, errors="strict")
    stream = io.StringIO(text, newline="")
    reader = csv.DictReader(stream, delimiter=profile.delimiter)
    if not reader.fieldnames or any(not field for field in reader.fieldnames):
        raise ValueError("Missing or invalid CSV header")
    lines = text.splitlines(keepends=True)
    last = reader.line_num
    for row in reader:
        end = reader.line_num
        raw = "".join(lines[last:end])
        last = end
        yield end, row, raw


def _rows_xlsx(data):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        infos = archive.infolist()
        if len(infos) > 200 or sum(i.file_size for i in infos) > 100 * 1024 * 1024:
            raise ValueError("Oversized XLSX archive")
    book = load_workbook(io.BytesIO(data), read_only=True, data_only=False)
    try:
        sheet = book.active
        iterator = sheet.iter_rows(values_only=True)
        headers = next(iterator, None)
        if not headers:
            raise ValueError("Missing XLSX header")
        headers = [str(h).strip() if h is not None else "" for h in headers]
        if len(set(headers)) != len(headers) or any(not h for h in headers):
            raise ValueError("Duplicate or empty XLSX headers")
        for index, cells in enumerate(iterator, start=2):
            if any(isinstance(v, str) and v.lstrip().startswith("=") for v in cells):
                raise ValueError(f"Excel formula in row {index} is not accepted")
            row = dict(zip(headers, cells))
            yield index, row, json.dumps(row, ensure_ascii=False, default=str)
    finally:
        book.close()


def inspect_file(data, filename, profile, source, kind, start_date, end_date):
    if kind not in {"external", "internal"}:
        raise ValueError("kind must be external or internal")
    if not data or len(data) > MAX_BYTES:
        raise ValueError("File is empty or too large (20 MiB limit)")
    suffix = Path(filename).suffix.lower()
    if suffix not in {".csv", ".xlsx"}:
        raise ValueError("Only CSV and XLSX are supported")
    if suffix == ".csv":
        iterator = _rows_csv(data, profile)
    else:
        iterator = _rows_xlsx(data)

    output, errors, natural_keys = [], [], set()
    total = 0
    cols = profile.mapping
    for index, row, raw in iterator:
        if index > MAX_ROWS + 1:
            raise ValueError("More than 250000 rows")
        try:
            def field(name):
                col = cols.get(name)
                return row.get(col) if col else None

            reference = str(field("reference") or "").strip()
            if not reference or len(reference) > 160:
                raise ValueError("Missing or oversized reference")
            booked = parse_date(field("date"), profile.date_format)
            if not start_date <= booked <= end_date:
                raise ValueError("Date outside selected period")
            amount = parse_minor(field("amount"), source.currency, profile.decimal_separator)
            account = str(field("account") or source.account).strip()
            if account != source.account:
                raise ValueError("Account differs from source")
            currency_value = str(field("currency") or source.currency).strip().upper()
            if currency_value != source.currency:
                raise ValueError("Currency differs from source")
            counterparty = str(field("counterparty") or "").strip()[:180]
            # Configure natural-key fields in profile. Identical amounts alone are never a key.
            parts = [kind, reference, booked.isoformat(), str(amount), account, currency_value]
            key = digest("\x1f".join(parts).encode("utf-8"))
            if key in natural_keys:
                raise ValueError("Duplicate natural key within file")
            natural_keys.add(key)
            normalized = {
                "reference": reference, "booked_date": booked.isoformat(),
                "amount_minor": amount, "account": account,
                "currency": currency_value, "counterparty": counterparty,
            }
            output.append({
                "row_number": index, "raw_text": raw, "natural_key": key,
                "normalized": normalized, "reference": reference,
                "booked_date": booked, "amount_minor": amount,
                "account": account, "currency": currency_value, "counterparty": counterparty,
            })
            total += amount
        except (ValueError, TypeError, KeyError) as exc:
            if len(errors) < 200:
                message = str(exc)
                issue_type = (
                    "wrong_currency" if "Currency differs" in message else
                    "duplicate" if "Duplicate natural key" in message else
                    "validation_error"
                )
                errors.append({"line": index, "type": issue_type, "message": message})
    return {
        "sha256": digest(data), "valid_rows": len(output), "total_minor": total,
        "errors": errors, "records": output, "filename": Path(filename).name,
    }
