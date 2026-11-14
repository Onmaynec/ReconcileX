import os
from datetime import datetime, timedelta, timezone
import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import select

from app.db import get_db
from app.models import SourceGrant, User

hasher = PasswordHasher()
bearer = HTTPBearer(auto_error=False)
ROLES = {"operator", "controller", "analyst", "admin", "auditor"}


def hash_password(value):
    return hasher.hash(value)


def verify_password(value, hashed):
    try:
        return hasher.verify(hashed, value)
    except (VerifyMismatchError, VerificationError):
        return False


def token_for(user):
    secret = os.getenv("JWT_SECRET")
    if not secret or len(secret) < 32:
        raise RuntimeError("JWT_SECRET must be set and contain at least 32 characters")
    payload = {
        "sub": str(user.id), "ver": int(user.token_epoch or 0),
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=8),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    db=Depends(get_db),
):
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(401, "Authentication required")
    try:
        payload = jwt.decode(credentials.credentials, os.environ["JWT_SECRET"], algorithms=["HS256"])
        from uuid import UUID
        user = db.get(User, UUID(payload["sub"]))
    except (jwt.PyJWTError, KeyError, ValueError, TypeError):
        raise HTTPException(401, "Invalid or expired token")
    if not user or not user.enabled:
        raise HTTPException(401, "Account disabled or not found")
    if not isinstance(payload.get("ver"), int) or payload["ver"] != user.token_epoch:
        raise HTTPException(401, "Session revoked")
    return user


def require_role(user, *roles):
    if user.role not in roles:
        raise HTTPException(403, "Operation forbidden for this role")


def ensure_source(db, user, source_id):
    if user.role == "admin":
        return
    grant = db.scalar(select(SourceGrant.id).where(
        SourceGrant.user_id == user.id, SourceGrant.source_id == source_id
    ))
    if not grant:
        raise HTTPException(403, "No access to this data source")
