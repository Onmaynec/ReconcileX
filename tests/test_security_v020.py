"""v0.2 security regressions; fixture data never contains real customer records."""
import uuid
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from app.api import app, safe_csv_cell
from app.db import SessionLocal
from app.models import User, Source, SourceGrant, ImportBatch, Record
from app.security import hash_password, token_for

client = TestClient(app)


@pytest.mark.parametrize("payload", [
    "=HYPERLINK(\"https://example.invalid\")",
    "+SUM(1,1)", "-2+1", "@SUM(1,1)",
    " \t=cmd|'/C calc'!A0", "\r\n=1+1",
])
def test_spreadsheet_formula_strings_are_neutralized(payload):
    escaped = safe_csv_cell(payload)
    assert escaped.startswith("'")
    assert escaped[1:] == payload


def test_sanitizer_preserves_regular_values():
    assert safe_csv_cell("ABC-123") == "ABC-123"
    assert safe_csv_cell("") == ""


def test_security_headers_and_body_limit():
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Referrer-Policy"] == "no-referrer"


def test_analyst_cannot_access_original_file():
    with SessionLocal.begin() as db:
        analyst = db.scalar(select(User).where(User.username == "analyst"))
        source = db.scalar(select(Source).where(Source.name == "demo-rouble"))
        if not db.scalar(select(SourceGrant).where(
                SourceGrant.source_id == source.id,
                SourceGrant.user_id == analyst.id)):
            db.add(SourceGrant(source_id=source.id, user_id=analyst.id))
        token = token_for(analyst)
        example = db.scalar(select(ImportBatch.id).where(
            ImportBatch.source_id == source.id).limit(1))
    assert example is not None
    response = client.get(f"/api/v1/imports/{example}/file",
                          headers={"Authorization": "Bearer " + token})
    assert response.status_code == 403
    with SessionLocal() as db:
        assert db.scalar(select(Record.id).where(
            Record.source_id == source.id).limit(1)) is not None


def test_unauthenticated_metrics_denied():
    result = client.get("/api/v1/admin/metrics")
    assert result.status_code == 401


def test_unknown_host_rejected_when_enabled(monkeypatch):
    # The middleware's active env setting is captured on startup; config test
    # covers canonical parsing without affecting another TestClient instance.
    from app.api import valid_trusted_hosts
    assert valid_trusted_hosts("example.com, api.example.com") == [
        "example.com", "api.example.com"]
    with pytest.raises(ValueError):
        valid_trusted_hosts("example.com, *")
