"""account deletion: cv_parse_results remember the candidate (ADR 0014)

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Nullable: rows parsed before this revision only match a purge by cv_id.
    op.execute("ALTER TABLE cv_parse_results ADD COLUMN candidate_id uuid")
    op.execute("CREATE INDEX cv_parse_results_candidate ON cv_parse_results (candidate_id)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS cv_parse_results_candidate")
    op.execute("ALTER TABLE cv_parse_results DROP COLUMN IF EXISTS candidate_id")
