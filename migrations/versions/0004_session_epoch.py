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
    # Revision 0001 uses current ORM metadata when creating a fresh database.
    # A fresh installation already gets this column there; an existing v0.3
    # database must receive it here without a duplicate-column failure.
    fields = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("users")}
    if "token_epoch" not in fields:
        op.add_column("users", sa.Column(
            "token_epoch", sa.Integer(), nullable=False, server_default="0"))


def downgrade():
    op.drop_column("users", "token_epoch")
