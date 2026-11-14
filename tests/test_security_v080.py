"""v0.8 security edge and authorization contract tests."""
from types import SimpleNamespace
import uuid

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.api import app
from app.db import SessionLocal
from app.models import User
from app.security import current_user, hash_password, token_for, trusted_client_ip

client = TestClient(app)


def request_from(peer, headers=None):
    return SimpleNamespace(
        client=SimpleNamespace(host=peer),
        headers=headers or {},
    )


def test_forwarded_ip_is_ignored_without_trusted_proxy(monkeypatch):
    monkeypatch.delenv("TRUSTED_PROXY_CIDRS", raising=False)
    request = request_from("127.0.0.1", {"X-Real-IP": "203.0.113.19"})
    assert trusted_client_ip(request) == "127.0.0.1"


def test_forwarded_ip_is_used_only_from_the_pinned_proxy(monkeypatch):
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.29.240.10/32")
    request = request_from("172.29.240.10", {"X-Real-IP": "203.0.113.19"})
    assert trusted_client_ip(request) == "203.0.113.19"


def test_forwarded_ip_from_an_untrusted_peer_cannot_change_rate_limit_key(monkeypatch):
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.29.240.10/32")
    request = request_from("172.29.240.11", {"X-Real-IP": "203.0.113.19"})
    assert trusted_client_ip(request) == "172.29.240.11"


def test_invalid_forwarded_ip_falls_back_to_trusted_proxy(monkeypatch):
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.29.240.10/32")
    request = request_from("172.29.240.10", {"X-Real-IP": "203.0.113.1, 10.0.0.1"})
    assert trusted_client_ip(request) == "172.29.240.10"


def test_invalid_trusted_proxy_configuration_is_rejected(monkeypatch):
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "not-a-network")
    with pytest.raises(RuntimeError, match="TRUSTED_PROXY_CIDRS"):
        trusted_client_ip(request_from("172.29.240.10", {"X-Real-IP": "203.0.113.19"}))


def test_every_api_route_except_login_requires_current_user():
    public_paths = {"/api/v1/auth/token"}
    protected = [
        route for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith("/api/v1/")
        and route.path not in public_paths
    ]
    assert protected
    for route in protected:
        assert any(dependency.call is current_user
                   for dependency in route.dependant.dependencies), route.path


@pytest.fixture
def role_accounts():
    suffix = uuid.uuid4().hex[:12]
    with SessionLocal.begin() as db:
        admin = db.scalar(select(User).where(User.username == "admin"))
        assert admin is not None
        users = {
            role: User(
                username=f"v080-{role}-{suffix}",
                role=role,
                password_hash=hash_password("v080-test-password-492801"),
            )
            for role in ("operator", "controller", "analyst", "auditor")
        }
        db.add_all(users.values())
        db.flush()
        accounts = {"admin": admin, **users}
        return {
            "ids": {role: account.id for role, account in accounts.items()},
            "tokens": {role: token_for(account) for role, account in accounts.items()},
        }


def auth(accounts, role):
    return {"Authorization": "Bearer " + accounts["tokens"][role]}


def test_admin_inspection_routes_reject_every_non_admin_role(role_accounts):
    target_id = role_accounts["ids"]["operator"]
    paths = (
        "/api/v1/admin/users",
        "/api/v1/admin/metrics",
        f"/api/v1/admin/grants?user_id={target_id}",
    )
    for role in ("operator", "controller", "analyst", "auditor"):
        for path in paths:
            response = client.get(path, headers=auth(role_accounts, role))
            assert response.status_code == 403, (role, path, response.text)

    assert client.get("/api/v1/admin/users",
                      headers=auth(role_accounts, "admin")).status_code == 200
    assert client.get("/api/v1/admin/metrics",
                      headers=auth(role_accounts, "admin")).status_code == 200
    assert client.get(f"/api/v1/admin/grants?user_id={target_id}",
                      headers=auth(role_accounts, "admin")).status_code == 200


def test_security_headers_are_present_on_public_health_response():
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Referrer-Policy"] == "no-referrer"
    assert response.headers["Cache-Control"] == "no-store"
