"""Postgres result store: idempotency + outcome + outbox in one transaction."""

from sqlalchemy import Engine, bindparam, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

from ai_service.adapters import outbox
from ai_service.domain.events import CvUploadedPayload, Envelope
from ai_service.domain.models import ParseOutcome, ParseSuccess

_IS_PROCESSED = text("SELECT 1 FROM processed_events WHERE event_id = :event_id")
_RECORD_EVENT = text(
    "INSERT INTO processed_events (event_id, processed_at) VALUES (:event_id, now()) "
    "ON CONFLICT (event_id) DO NOTHING"
)
# A CV whose matcher replica is a DELETED tombstone (account deletion, ADR
# 0014) gets neither a stored result nor a cv.parsed carrying its content.
_CV_DELETED = text("SELECT 1 FROM match_cvs WHERE cv_id = :cv_id AND status = 'DELETED'")
_UPSERT_RESULT = text(
    "INSERT INTO cv_parse_results "
    "(cv_id, attempt_id, status, reason, parsed, model_version, text_chars, truncated, candidate_id) "
    "VALUES (:cv_id, :attempt_id, :status, :reason, :parsed, :model_version, :text_chars, :truncated, "
    ":candidate_id) "
    "ON CONFLICT (cv_id, attempt_id) DO UPDATE SET status = EXCLUDED.status, reason = EXCLUDED.reason, "
    "parsed = EXCLUDED.parsed, model_version = EXCLUDED.model_version, "
    "text_chars = EXCLUDED.text_chars, truncated = EXCLUDED.truncated"
).bindparams(bindparam("parsed", type_=JSONB))
_PENDING = text("SELECT count(*) FROM outbox WHERE published_at IS NULL")


def make_engine(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True)


class PgResultStore:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_processed(self, event_id: str) -> bool:
        with self._engine.connect() as conn:
            return conn.execute(_IS_PROCESSED, {"event_id": event_id}).first() is not None

    def commit_outcome(
        self, event_id: str, cv: CvUploadedPayload, outcome: ParseOutcome, result: Envelope
    ) -> bool:
        with self._engine.begin() as conn:
            if conn.execute(_RECORD_EVENT, {"event_id": event_id}).rowcount == 0:
                conn.rollback()
                return False
            if conn.execute(_CV_DELETED, {"cv_id": cv.cv_id}).first() is not None:
                return True
            if isinstance(outcome, ParseSuccess):
                status, reason, parsed = "PARSED", None, outcome.parsed.model_dump(mode="json")
            else:
                status, reason, parsed = "FAILED", outcome.reason, None
            conn.execute(
                _UPSERT_RESULT,
                {
                    "cv_id": cv.cv_id,
                    "attempt_id": cv.attempt_id,
                    "status": status,
                    "reason": reason,
                    "parsed": parsed,
                    "model_version": outcome.model_version,
                    "text_chars": outcome.text_chars,
                    "truncated": outcome.truncated,
                    "candidate_id": cv.candidate_id,
                },
            )
            outbox.write(conn, result, aggregate_id=str(cv.cv_id))
        return True


def outbox_pending(engine: Engine) -> int:
    with engine.connect() as conn:
        return int(conn.execute(_PENDING).scalar_one())
