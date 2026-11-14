"""Import rejection provenance and issue owner assignment.

Revision ID: 0003_exceptions
Revises: 0002_history_rate
"""
from alembic import op
from app.models import ImportRejection, IssueAssignment

revision = "0003_exceptions"
down_revision = "0002_history_rate"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    ImportRejection.__table__.create(bind=bind)
    IssueAssignment.__table__.create(bind=bind)


def downgrade():
    bind = op.get_bind()
    IssueAssignment.__table__.drop(bind=bind)
    ImportRejection.__table__.drop(bind=bind)
