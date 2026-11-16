"""init: cv_parse_results, processed_events, outbox

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

# processed_events and outbox mirror idempotency.SchemaSQL and outbox.SchemaSQL (Go pkg/).
_CV_PARSE_RESULTS = """
CREATE TABLE cv_parse_results (
    cv_id uuid NOT NULL,
    attempt_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('PARSED', 'FAILED')),
    reason text,
    parsed jsonb,
    model_version text,
    text_chars int NOT NULL,
    truncated bool NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cv_id, attempt_id)
)
"""

_PROCESSED_EVENTS = """
CREATE TABLE processed_events (
    event_id text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
)
"""

_OUTBOX = """
CREATE TABLE IF NOT EXISTS outbox (
    id uuid PRIMARY KEY,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
)
"""

_OUTBOX_INDEX = (
    "CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL"
)


def upgrade() -> None:
    op.execute(_CV_PARSE_RESULTS)
    op.execute(_PROCESSED_EVENTS)
    op.execute(_OUTBOX)
    op.execute(_OUTBOX_INDEX)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS outbox")
    op.execute("DROP TABLE IF EXISTS processed_events")
    op.execute("DROP TABLE IF EXISTS cv_parse_results")
