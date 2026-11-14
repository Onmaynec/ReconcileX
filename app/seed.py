"""Idempotent bootstrap with only synthetic/demo credentials and sources."""
import os
from datetime import date, timedelta
from sqlalchemy import select

from app.db import SessionLocal
from app.models import ImportProfile, MatchRule, Period, Source, SourceGrant, User
from app.security import hash_password

ROLES = ("operator", "controller", "analyst", "auditor", "admin")


def main():
    password = os.environ.get("DEMO_PASSWORD")
    if not password or len(password) < 12:
        raise RuntimeError("Set DEMO_PASSWORD to a unique value of at least 12 characters")
    with SessionLocal.begin() as db:
        users = {}
        for role in ROLES:
            user = db.scalar(select(User).where(User.username == role))
            if not user:
                user = User(username=role, role=role, password_hash=hash_password(password))
                db.add(user)
                db.flush()
            users[role] = user
        for name, account, currency in (
            ("demo-rouble", "DEMO-RUB-001", "RUB"),
            ("demo-euro", "DEMO-EUR-001", "EUR"),
        ):
            source = db.scalar(select(Source).where(Source.name == name))
            if not source:
                source = Source(name=name, account=account, currency=currency)
                db.add(source)
                db.flush()
            for user in users.values():
                if not db.scalar(select(SourceGrant.id).where(
                    SourceGrant.source_id == source.id, SourceGrant.user_id == user.id)):
                    db.add(SourceGrant(source_id=source.id, user_id=user.id))
            db.flush()
            if not db.scalar(select(ImportProfile.id).where(
                ImportProfile.source_id == source.id, ImportProfile.name == "demo-csv")):
                db.add(ImportProfile(
                    source_id=source.id, name="demo-csv", encoding="utf-8-sig",
                    delimiter=",", date_format="%Y-%m-%d",
                    mapping={"reference": "reference", "date": "date",
                             "amount": "amount", "counterparty": "counterparty"}))
            if not db.scalar(select(MatchRule.id).where(
                MatchRule.source_id == source.id, MatchRule.rule_key == "exact-reference")):
                db.add(MatchRule(
                    source_id=source.id, rule_key="exact-reference", version=1,
                    priority=100, day_window=2, tolerance_minor=0,
                    require_reference=True))
        for year, month in ((2026, 8), (2026, 9), (2026, 10)):
            start = date(year, month, 1)
            end = date(year, month + 1, 1) - timedelta(days=1)
            if not db.scalar(select(Period.id).where(Period.name == f"{year}-{month:02}")):
                db.add(Period(name=f"{year}-{month:02}", start_date=start, end_date=end))
    print("Demo users, sources, rules and periods created")


if __name__ == "__main__":
    main()
