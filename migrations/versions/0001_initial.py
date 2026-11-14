"""Initial financial schema.

Revision ID: 0001_initial
Revises:
Create Date: 2026-11-14
"""
from alembic import op
from sqlalchemy import text
from app.db import Base
import app.models  # noqa: F401

INITIAL_TABLES = (
    "users", "data_sources", "source_grants", "import_profiles",
    "periods", "import_batches", "records", "match_rules",
    "match_groups", "match_members", "reconcile_issues",
    "reconcile_jobs", "audit_events",
)

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    original = set(INITIAL_TABLES)
    for table in Base.metadata.sorted_tables:
        if table.name in original:
            table.create(bind=bind, checkfirst=False)
    if bind.dialect.name == "postgresql":
        op.execute(text("""
CREATE OR REPLACE FUNCTION prohibit_immutable_financial_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Immutable financial record: use an audit event or cancellation';
END;
$$;
"""))
        for name in ("records", "import_batches", "audit_events"):
            op.execute(text(f"""
CREATE TRIGGER trg_immutable_{name}
BEFORE UPDATE OR DELETE ON {name}
FOR EACH ROW EXECUTE FUNCTION prohibit_immutable_financial_mutation()
"""))


def downgrade():
    bind = op.get_bind()
    original = set(INITIAL_TABLES)
    for table in reversed(Base.metadata.sorted_tables):
        if table.name in original:
            table.drop(bind=bind, checkfirst=False)
    if bind.dialect.name == "postgresql":
        op.execute(text("DROP FUNCTION IF EXISTS prohibit_immutable_financial_mutation()"))
