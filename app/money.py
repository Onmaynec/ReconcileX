from decimal import Decimal, InvalidOperation
from datetime import date, datetime
import re

SCALE = {"RUB": 2, "EUR": 2, "USD": 2, "GBP": 2, "JPY": 0}


def parse_minor(value, currency="RUB", decimal_separator="."):
    """Strict decimal conversion: never perform floating point arithmetic."""
    if currency not in SCALE:
        raise ValueError("Unsupported currency")
    if value is None or isinstance(value, bool):
        raise ValueError("Amount missing")
    raw = str(value).strip().replace("\u00a0", "").replace(" ", "")
    if decimal_separator == ",":
        if raw.count(",") > 1 or "." in raw:
            raise ValueError("Invalid decimal separator")
        raw = raw.replace(",", ".")
    if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", raw):
        raise ValueError("Invalid decimal amount")
    try:
        amount = Decimal(raw)
        multiplier = Decimal(10) ** SCALE[currency]
        result = amount * multiplier
    except InvalidOperation as exc:
        raise ValueError("Invalid decimal amount") from exc
    if not result.is_finite() or result != result.to_integral_value():
        raise ValueError("Amount has unsupported fractional minor units")
    if abs(result) > 9_000_000_000_000_000_000:
        raise ValueError("Amount exceeds supported range")
    return int(result)


def parse_date(value, fmt):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value).strip(), fmt).date()
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid date") from exc
