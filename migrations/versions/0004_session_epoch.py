"""Revoke JWT sessions after password reset and account disable.

Revision ID: 0004_session_epoch
Revises: 0003_exceptions
"""
from alembic import op
import sqlalchemy as sa

revision = "0004_session_epoch"
down_revision = "0003_exceptions"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("users", sa.Column(
        "token_epoch", sa.Integer(), nullable=False, server_default="0"))


def downgrade():
    op.drop_column("users", "token_epoch")
