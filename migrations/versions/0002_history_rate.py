"""Closure history and auth rate guard.

Revision ID: 0002_history_rate
Revises: 0001_initial
"""
from alembic import op
from sqlalchemy import text
from app.models import PeriodSnapshot, AuthAttempt

revision = "0002_history_rate"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    PeriodSnapshot.__table__.create(bind=bind)
    AuthAttempt.__table__.create(bind=bind)
    if bind.dialect.name == "postgresql":
        op.execute(text("""
CREATE TRIGGER trg_immutable_period_snapshots
BEFORE UPDATE OR DELETE ON period_snapshots
FOR EACH ROW EXECUTE FUNCTION prohibit_immutable_financial_mutation()
"""))


def downgrade():
    bind = op.get_bind()
    AuthAttempt.__table__.drop(bind=bind)
    PeriodSnapshot.__table__.drop(bind=bind)
