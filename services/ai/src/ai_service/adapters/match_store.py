"""Postgres store of the matching replicas and results (ADR 0010).

Write methods take the caller's Connection so one event is applied in one
transaction (replica + results + processed_events + outbox); read methods open
their own connection (gRPC side).
"""

import json
from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager
from dataclasses import asdict
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, Engine, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TEXT
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from ai_service.adapters import outbox
from ai_service.domain.events import Envelope
from ai_service.domain.matching import Breakdown, CvProfile, JobProfile, MatchRow
from ai_service.domain.models import ParsedCV

_IS_PROCESSED = text("SELECT 1 FROM processed_events WHERE event_id = :event_id")
_RECORD_EVENT = text(
    "INSERT INTO processed_events (event_id, processed_at) VALUES (:event_id, now()) "
    "ON CONFLICT (event_id) DO NOTHING"
)
_UPSERT_JOB = text(
    "INSERT INTO match_jobs (job_id, recruiter_id, version, status, doc, updated_at) "
    "VALUES (:job_id, :recruiter_id, :version, :status, :doc, now()) "
    "ON CONFLICT (job_id) DO UPDATE SET recruiter_id = EXCLUDED.recruiter_id, version = EXCLUDED.version, "
    "status = EXCLUDED.status, doc = EXCLUDED.doc, updated_at = now() "
    "WHERE match_jobs.version < EXCLUDED.version"
).bindparams(bindparam("doc", type_=JSONB))
_SELECT_JOB = text("SELECT job_id, recruiter_id, version, status, doc FROM match_jobs WHERE job_id = :id")
_OPEN_JOBS = text(
    "SELECT job_id, recruiter_id, version, status, doc FROM match_jobs "
    "WHERE status = 'open' AND job_id = ANY(:ids)"
).bindparams(bindparam("ids", type_=ARRAY(PG_UUID(as_uuid=True))))
_UPSERT_CV = text(
    "INSERT INTO match_cvs (cv_id, candidate_id, version, status, parsed, parsed_at, updated_at) "
    "VALUES (:cv_id, :candidate_id, :version, :status, :parsed, :parsed_at, now()) "
    "ON CONFLICT (cv_id) DO UPDATE SET candidate_id = EXCLUDED.candidate_id, version = EXCLUDED.version, "
    "status = EXCLUDED.status, parsed = EXCLUDED.parsed, parsed_at = EXCLUDED.parsed_at, updated_at = now() "
    "WHERE match_cvs.version < EXCLUDED.version"
).bindparams(bindparam("parsed", type_=JSONB))
_SELECT_CV = text(
    "SELECT cv_id, candidate_id, version, status, parsed, parsed_at FROM match_cvs WHERE cv_id = :id"
)
_SELECT_CVS = text(
    "SELECT cv_id, candidate_id, version, status, parsed, parsed_at FROM match_cvs WHERE cv_id = ANY(:ids)"
).bindparams(bindparam("ids", type_=ARRAY(PG_UUID(as_uuid=True))))
_CANDIDATE_CVS = text(
    "SELECT cv_id, status, parsed_at FROM match_cvs WHERE candidate_id = :candidate_id "
    "ORDER BY parsed_at DESC NULLS LAST, cv_id DESC FOR UPDATE"
)
_SET_ACTIVE = text(
    "UPDATE match_cvs SET active = (cv_id IS NOT DISTINCT FROM :active) WHERE candidate_id = :candidate_id "
    "AND active IS DISTINCT FROM (cv_id IS NOT DISTINCT FROM :active)"
)
_UPSERT_RESULT = text(
    "INSERT INTO match_results (job_id, cv_id, candidate_id, score, breakdown, model_version, computed_at) "
    "VALUES (:job_id, :cv_id, :candidate_id, :score, :breakdown, :model_version, now()) "
    "ON CONFLICT (job_id, cv_id) DO UPDATE SET candidate_id = EXCLUDED.candidate_id, score = EXCLUDED.score, "
    "breakdown = EXCLUDED.breakdown, model_version = EXCLUDED.model_version, computed_at = now()"
).bindparams(bindparam("breakdown", type_=JSONB))
_DELETE_JOB_RESULTS = text("DELETE FROM match_results WHERE job_id = :job_id")
_DELETE_CV_RESULTS = text("DELETE FROM match_results WHERE cv_id = ANY(:ids)").bindparams(
    bindparam("ids", type_=ARRAY(PG_UUID(as_uuid=True)))
)
# --- account deletion (ADR 0014) ---
# A purged CV stays as a DELETED tombstone with the highest version, so no
# cv.updated delivered late can bring its content back.
_TOMBSTONE_VERSION = 9_223_372_036_854_775_807
_LOCK_CANDIDATE_CVS = text("SELECT cv_id FROM match_cvs WHERE candidate_id = :candidate_id FOR UPDATE")
_TOMBSTONE_CANDIDATE_CVS = text(
    "UPDATE match_cvs SET status = 'DELETED', parsed = NULL, parsed_at = NULL, active = false, "
    "version = :version, updated_at = now() WHERE candidate_id = :candidate_id"
)
_DELETE_CANDIDATE_RESULTS = text("DELETE FROM match_results WHERE candidate_id = :candidate_id")
_DELETE_CANDIDATE_PARSES = text(
    "DELETE FROM cv_parse_results WHERE candidate_id = :candidate_id OR cv_id = ANY(:ids)"
).bindparams(bindparam("ids", type_=ARRAY(PG_UUID(as_uuid=True))))
_DELETE_CV_PARSES = text("DELETE FROM cv_parse_results WHERE cv_id = :cv_id")
# Published cv.parsed rows still hold the parsed CV.
_DELETE_CV_OUTBOX = text(
    "DELETE FROM outbox WHERE aggregate_id = ANY(:keys) AND event_type = 'cv.parsed'"
).bindparams(bindparam("keys", type_=ARRAY(TEXT)))
_CANDIDATE_PARSED_CVS = text("SELECT cv_id FROM cv_parse_results WHERE candidate_id = :candidate_id")
_ANONYMISE_RECRUITER = text(
    "UPDATE match_jobs SET recruiter_id = '00000000-0000-0000-0000-000000000000', updated_at = now() "
    "WHERE recruiter_id = :recruiter_id"
)

_TOP = text(
    "SELECT r.job_id, r.cv_id, r.candidate_id, r.score, r.breakdown, r.model_version "
    "FROM match_results r JOIN match_cvs c ON c.cv_id = r.cv_id AND c.active "
    "WHERE r.job_id = :job_id ORDER BY r.score DESC, r.cv_id LIMIT :k"
)


def _job_doc(j: JobProfile) -> dict[str, Any]:
    return {
        "title": j.title,
        "description": j.description,
        "required_skills": j.required_skills,
        "nice_to_have_skills": j.nice_to_have_skills,
        "level": j.level,
        "location": j.location,
        "salary_min": j.salary_min,
        "salary_max": j.salary_max,
    }


def _job(row: Any) -> JobProfile:
    d = row.doc
    return JobProfile(
        job_id=row.job_id,
        recruiter_id=row.recruiter_id,
        version=row.version,
        status=row.status,
        title=d["title"],
        description=d["description"],
        required_skills=list(d["required_skills"]),
        nice_to_have_skills=list(d["nice_to_have_skills"]),
        level=d["level"],
        location=d["location"],
        salary_min=d["salary_min"],
        salary_max=d["salary_max"],
    )


def _cv(row: Any) -> CvProfile:
    parsed = ParsedCV.model_validate_json(json.dumps(row.parsed)) if row.parsed is not None else None
    return CvProfile(
        cv_id=row.cv_id,
        candidate_id=row.candidate_id,
        version=row.version,
        status=row.status,
        parsed=parsed,
        parsed_at=row.parsed_at,
    )


def _row(r: Any) -> MatchRow:
    return MatchRow(
        job_id=r.job_id,
        cv_id=r.cv_id,
        candidate_id=r.candidate_id,
        score=float(r.score),
        breakdown=Breakdown(**r.breakdown),
        model_version=r.model_version,
    )


class PgMatchStore:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def transaction(self) -> AbstractContextManager[Connection]:
        return self._engine.begin()

    def is_processed(self, event_id: str) -> bool:
        with self._engine.connect() as conn:
            return conn.execute(_IS_PROCESSED, {"event_id": event_id}).first() is not None

    # --- writes (caller's transaction) ---

    def write_outbox(self, conn: Connection, env: Envelope, aggregate_id: str) -> None:
        outbox.write(conn, env, aggregate_id=aggregate_id)

    def record_event(self, conn: Connection, event_id: str) -> bool:
        """False when the event was already processed."""
        return conn.execute(_RECORD_EVENT, {"event_id": event_id}).rowcount == 1

    def upsert_job(self, conn: Connection, job: JobProfile) -> bool:
        """Stores job unless a same-or-newer version is stored; True when applied."""
        params = {
            "job_id": job.job_id,
            "recruiter_id": job.recruiter_id,
            "version": job.version,
            "status": job.status,
            "doc": _job_doc(job),
        }
        return conn.execute(_UPSERT_JOB, params).rowcount == 1

    def upsert_cv(self, conn: Connection, cv: CvProfile) -> bool:
        params = {
            "cv_id": cv.cv_id,
            "candidate_id": cv.candidate_id,
            "version": cv.version,
            "status": cv.status,
            "parsed": cv.parsed.model_dump(mode="json") if cv.parsed is not None else None,
            "parsed_at": cv.parsed_at,
        }
        return conn.execute(_UPSERT_CV, params).rowcount == 1

    def refresh_active(self, conn: Connection, candidate_id: UUID) -> tuple[UUID | None, list[UUID]]:
        """Marks the candidate's newest PARSED CV active; returns it and the other PARSED CVs."""
        rows = conn.execute(_CANDIDATE_CVS, {"candidate_id": candidate_id}).all()
        parsed = [r.cv_id for r in rows if r.status == "PARSED" and r.parsed_at is not None]
        active = parsed[0] if parsed else None
        conn.execute(_SET_ACTIVE, {"candidate_id": candidate_id, "active": active})
        return active, parsed[1:]

    def replace_job_results(self, conn: Connection, job_id: UUID, rows: Sequence[MatchRow]) -> None:
        conn.execute(_DELETE_JOB_RESULTS, {"job_id": job_id})
        self.upsert_results(conn, rows)

    def upsert_results(self, conn: Connection, rows: Iterable[MatchRow]) -> None:
        params = [
            {
                "job_id": r.job_id,
                "cv_id": r.cv_id,
                "candidate_id": r.candidate_id,
                "score": r.score,
                "breakdown": asdict(r.breakdown),
                "model_version": r.model_version,
            }
            for r in rows
        ]
        if params:
            conn.execute(_UPSERT_RESULT, params)

    def delete_results_for_job(self, conn: Connection, job_id: UUID) -> None:
        conn.execute(_DELETE_JOB_RESULTS, {"job_id": job_id})

    def delete_results_for_cvs(self, conn: Connection, cv_ids: Sequence[UUID]) -> None:
        if cv_ids:
            conn.execute(_DELETE_CV_RESULTS, {"ids": list(cv_ids)})

    def purge_candidate(self, conn: Connection, candidate_id: UUID) -> list[UUID]:
        """Tombstones the candidate's CVs and deletes its results, parse results and
        cv.parsed outbox rows; returns the tombstoned CV ids (their vectors go too)."""
        cv_ids = [r.cv_id for r in conn.execute(_LOCK_CANDIDATE_CVS, {"candidate_id": candidate_id})]
        parsed_ids = [r.cv_id for r in conn.execute(_CANDIDATE_PARSED_CVS, {"candidate_id": candidate_id})]
        conn.execute(_TOMBSTONE_CANDIDATE_CVS, {"candidate_id": candidate_id, "version": _TOMBSTONE_VERSION})
        conn.execute(_DELETE_CANDIDATE_RESULTS, {"candidate_id": candidate_id})
        conn.execute(_DELETE_CANDIDATE_PARSES, {"candidate_id": candidate_id, "ids": cv_ids})
        keys = sorted({str(i) for i in (*cv_ids, *parsed_ids)})
        if keys:
            conn.execute(_DELETE_CV_OUTBOX, {"keys": keys})
        return cv_ids

    def forget_cv(self, conn: Connection, cv_id: UUID) -> None:
        """Drops a deleted CV's parse results and cv.parsed outbox rows."""
        conn.execute(_DELETE_CV_PARSES, {"cv_id": cv_id})
        conn.execute(_DELETE_CV_OUTBOX, {"keys": [str(cv_id)]})

    def anonymise_recruiter(self, conn: Connection, recruiter_id: UUID) -> None:
        conn.execute(_ANONYMISE_RECRUITER, {"recruiter_id": recruiter_id})

    # --- reads ---

    def get_job(self, conn: Connection, job_id: UUID) -> JobProfile | None:
        row = conn.execute(_SELECT_JOB, {"id": job_id}).first()
        return _job(row) if row else None

    def get_cv(self, conn: Connection, cv_id: UUID) -> CvProfile | None:
        row = conn.execute(_SELECT_CV, {"id": cv_id}).first()
        return _cv(row) if row else None

    def load_job(self, job_id: UUID) -> JobProfile | None:
        with self._engine.connect() as conn:
            return self.get_job(conn, job_id)

    def load_cv(self, cv_id: UUID) -> CvProfile | None:
        with self._engine.connect() as conn:
            return self.get_cv(conn, cv_id)

    def load_cvs(self, cv_ids: Sequence[UUID]) -> dict[UUID, CvProfile]:
        if not cv_ids:
            return {}
        with self._engine.connect() as conn:
            return {r.cv_id: _cv(r) for r in conn.execute(_SELECT_CVS, {"ids": list(cv_ids)})}

    def open_jobs(self, job_ids: Sequence[UUID]) -> dict[UUID, JobProfile]:
        if not job_ids:
            return {}
        with self._engine.connect() as conn:
            return {r.job_id: _job(r) for r in conn.execute(_OPEN_JOBS, {"ids": list(job_ids)})}

    def top_candidates(self, job_id: UUID, k: int) -> list[MatchRow]:
        """Results of the job's active CVs, best first."""
        with self._engine.connect() as conn:
            return [_row(r) for r in conn.execute(_TOP, {"job_id": job_id, "k": k})]
