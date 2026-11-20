"""matching: job/CV replicas and precomputed match results (ADR 0010)

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

_MATCH_JOBS = """
CREATE TABLE match_jobs (
    job_id uuid PRIMARY KEY,
    recruiter_id uuid NOT NULL,
    version bigint NOT NULL,
    status text NOT NULL,
    doc jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
)
"""

_MATCH_CVS = """
CREATE TABLE match_cvs (
    cv_id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL,
    version bigint NOT NULL,
    status text NOT NULL,
    parsed jsonb,
    parsed_at timestamptz,
    active boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
)
"""

_MATCH_RESULTS = """
CREATE TABLE match_results (
    job_id uuid NOT NULL,
    cv_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    score numeric(5,2) NOT NULL,
    breakdown jsonb NOT NULL,
    model_version text NOT NULL,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, cv_id)
)
"""


def upgrade() -> None:
    op.execute(_MATCH_JOBS)
    op.execute(_MATCH_CVS)
    op.execute("CREATE INDEX match_cvs_candidate ON match_cvs (candidate_id)")
    op.execute(_MATCH_RESULTS)
    op.execute("CREATE INDEX match_results_job_score ON match_results (job_id, score DESC)")
    op.execute("CREATE INDEX match_results_cv ON match_results (cv_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS match_results")
    op.execute("DROP TABLE IF EXISTS match_cvs")
    op.execute("DROP TABLE IF EXISTS match_jobs")
