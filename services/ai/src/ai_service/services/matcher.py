"""ai-matcher use case: keep job/CV replicas, vectors and precomputed matches (ADR 0010).

One event = one Postgres transaction (processed_events + replica + results +
outbox). Qdrant writes happen inside that transaction's scope, before commit:
they are idempotent (point id = job/CV id), so if Qdrant or the commit fails the
whole event is retried. Embeddings are computed before the transaction.
"""

import logging
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ValidationError

from ai_service.domain.events import (
    SOURCE_CANDIDATE,
    SOURCE_JOB,
    CvUpdatedPayload,
    Envelope,
    JobPayload,
    JobRefPayload,
    new_envelope,
)
from ai_service.domain.matching import CvProfile, JobProfile, MatchRow, cv_text, job_text, score
from ai_service.services.cv_uploaded_handler import PermanentError
from ai_service.services.embedding import Embedder

log = logging.getLogger(__name__)

JOB_EVENTS = ("job.created", "job.updated", "job.closed", "job.deleted")
CV_UPDATED = "cv.updated"
TOPICS = (*JOB_EVENTS, CV_UPDATED)
MATCH_COMPLETED = "match.completed"
POOL = 50
COMPLETED_TOP = 20
STATUS_OPEN = "open"
CV_PARSED = "PARSED"


class MatchStore(Protocol):
    def transaction(self) -> AbstractContextManager[Any]: ...
    def is_processed(self, event_id: str) -> bool: ...
    def record_event(self, conn: Any, event_id: str) -> bool: ...
    def upsert_job(self, conn: Any, job: JobProfile) -> bool: ...
    def upsert_cv(self, conn: Any, cv: CvProfile) -> bool: ...
    def get_job(self, conn: Any, job_id: UUID) -> JobProfile | None: ...
    def get_cv(self, conn: Any, cv_id: UUID) -> CvProfile | None: ...
    def refresh_active(self, conn: Any, candidate_id: UUID) -> tuple[UUID | None, list[UUID]]: ...
    def replace_job_results(self, conn: Any, job_id: UUID, rows: Sequence[MatchRow]) -> None: ...
    def upsert_results(self, conn: Any, rows: Sequence[MatchRow]) -> None: ...
    def delete_results_for_job(self, conn: Any, job_id: UUID) -> None: ...
    def delete_results_for_cvs(self, conn: Any, cv_ids: Sequence[UUID]) -> None: ...
    def write_outbox(self, conn: Any, env: Envelope, aggregate_id: str) -> None: ...
    def load_job(self, job_id: UUID) -> JobProfile | None: ...
    def load_cv(self, cv_id: UUID) -> CvProfile | None: ...
    def load_cvs(self, cv_ids: Sequence[UUID]) -> dict[UUID, CvProfile]: ...
    def open_jobs(self, job_ids: Sequence[UUID]) -> dict[UUID, JobProfile]: ...


class VectorStore(Protocol):
    def upsert_job(self, job_id: UUID, vector: list[float], *, recruiter_id: UUID) -> None: ...
    def upsert_cv(self, cv_id: UUID, vector: list[float], *, candidate_id: UUID, active: bool) -> None: ...
    def set_cv_active(self, *, active: Sequence[UUID], inactive: Sequence[UUID]) -> None: ...
    def delete_job(self, job_id: UUID) -> None: ...
    def delete_cv(self, cv_id: UUID) -> None: ...
    def get_cv_vector(self, cv_id: UUID) -> list[float] | None: ...
    def nearest_cvs(self, vector: list[float], limit: int) -> list[tuple[UUID, float]]: ...
    def nearest_jobs(self, vector: list[float], limit: int) -> list[tuple[UUID, float]]: ...


def _payload[M: BaseModel](model: type[M], env: Envelope) -> M:
    try:
        return model.model_validate(env.payload)
    except ValidationError:
        # Not chained or echoed: pydantic errors repeat payload values.
        raise PermanentError(f"{env.event_type} payload failed validation") from None


class MatchHandler:
    def __init__(
        self,
        store: MatchStore,
        vectors: VectorStore,
        embedder: Embedder,
        *,
        clock: Callable[[], datetime],
        trace_id: Callable[[], str],
    ) -> None:
        self._store = store
        self._vectors = vectors
        self._embedder = embedder
        self._clock = clock
        self._trace_id = trace_id

    def handle(self, env: Envelope) -> None:
        if env.event_version != 1:
            raise PermanentError("unexpected event version")
        if env.event_type in JOB_EVENTS:
            if env.source != SOURCE_JOB:
                raise PermanentError("unexpected event source")
            self._job_event(env)
        elif env.event_type == CV_UPDATED:
            if env.source != SOURCE_CANDIDATE:
                raise PermanentError("unexpected event source")
            p = _payload(CvUpdatedPayload, env)
            if (p.status == CV_PARSED) != (p.parsed is not None and p.parsed_at is not None):
                raise PermanentError("cv.updated parsed fields do not match status")
            self._cv_updated(env, p)
        else:
            raise PermanentError("unexpected event type")

    # --- jobs ---

    def _job_event(self, env: Envelope) -> None:
        if env.event_type in ("job.created", "job.updated"):
            p = _payload(JobPayload, env)
            job = JobProfile(
                job_id=p.job_id,
                recruiter_id=p.recruiter_id,
                version=p.version,
                status=p.status,
                title=p.title,
                description=p.description,
                required_skills=p.required_skills,
                nice_to_have_skills=p.nice_to_have_skills,
                level=p.level,
                location=p.location or None,
                salary_min=p.salary_min,
                salary_max=p.salary_max,
            )
        else:
            ref = _payload(JobRefPayload, env)
            current = self._store.load_job(ref.job_id)
            if current is None:
                # Closed/deleted before we ever saw it: keep a minimal tombstone.
                current = JobProfile(ref.job_id, UUID(int=0), 0, "", "", "", [], [], "", None, None, None)
            status = "closed" if env.event_type == "job.closed" else "deleted"
            job = JobProfile(**{**current.__dict__, "version": ref.version, "status": status})
        if self._store.is_processed(env.event_id) or self._stale_job(job):
            self._skip(env)
            return
        if job.status == STATUS_OPEN:
            self._match_job(env, job)
        else:
            with self._store.transaction() as conn:
                if not self._store.record_event(conn, env.event_id) or not self._store.upsert_job(conn, job):
                    return
                self._store.delete_results_for_job(conn, job.job_id)
                self._vectors.delete_job(job.job_id)
        log.info(
            "job matched", extra={"event_id": env.event_id, "job_id": str(job.job_id), "status": job.status}
        )

    def _stale_job(self, job: JobProfile) -> bool:
        current = self._store.load_job(job.job_id)
        return current is not None and current.version >= job.version

    def _match_job(self, env: Envelope, job: JobProfile) -> None:
        (vector,) = self._embedder.embed([job_text(job)])
        hits = self._vectors.nearest_cvs(vector, POOL)
        cvs = self._store.load_cvs([cv_id for cv_id, _ in hits])
        rows: list[MatchRow] = []
        for cv_id, sim in hits:
            cv = cvs.get(cv_id)
            if cv is None or cv.parsed is None:
                continue
            total, breakdown = score(sim, job, cv.parsed)
            rows.append(
                MatchRow(job.job_id, cv_id, cv.candidate_id, total, breakdown, self._embedder.model_version)
            )
        rows.sort(key=lambda r: (-r.score, str(r.cv_id)))
        with self._store.transaction() as conn:
            if not self._store.record_event(conn, env.event_id) or not self._store.upsert_job(conn, job):
                return
            self._store.replace_job_results(conn, job.job_id, rows)
            self._store.write_outbox(conn, self._completed(env, job, rows), str(job.job_id))
            self._vectors.upsert_job(job.job_id, vector, recruiter_id=job.recruiter_id)

    def _completed(self, env: Envelope, job: JobProfile, rows: Sequence[MatchRow]) -> Envelope:
        top = [
            {"candidate_id": str(r.candidate_id), "cv_id": str(r.cv_id), "score": r.score}
            for r in rows[:COMPLETED_TOP]
        ]
        payload = {"job_id": str(job.job_id), "model_version": self._embedder.model_version, "top": top}
        return new_envelope(
            MATCH_COMPLETED,
            payload,
            now=self._clock(),
            trace_id=self._trace_id(),
            correlation_id=env.correlation_id,
        )

    # --- CVs ---

    def _cv_updated(self, env: Envelope, p: CvUpdatedPayload) -> None:
        cv = CvProfile(p.cv_id, p.candidate_id, p.version, p.status, p.parsed, p.parsed_at)
        current = self._store.load_cv(cv.cv_id)
        if self._store.is_processed(env.event_id) or (current is not None and current.version >= cv.version):
            self._skip(env)
            return
        vector: list[float] | None = None
        rows: list[MatchRow] = []
        if cv.parsed is not None:
            (vector,) = self._embedder.embed([cv_text(cv.parsed)])
            rows = self._score_cv(cv, vector)
        with self._store.transaction() as conn:
            if not self._store.record_event(conn, env.event_id) or not self._store.upsert_cv(conn, cv):
                return
            active, others = self._store.refresh_active(conn, cv.candidate_id)
            self._store.delete_results_for_cvs(conn, [cv.cv_id])
            self._store.upsert_results(conn, rows)
            if active is not None and active != cv.cv_id:
                # Another CV of the candidate is (or becomes) active: rescore it from
                # its stored vector, since jobs matched while it was inactive skipped it.
                self._rescore_cv(conn, active)
            if vector is not None:
                self._vectors.upsert_cv(
                    cv.cv_id, vector, candidate_id=cv.candidate_id, active=active == cv.cv_id
                )
            else:
                self._vectors.delete_cv(cv.cv_id)
            self._vectors.set_cv_active(
                active=[active] if active is not None and active != cv.cv_id else [],
                inactive=[o for o in others if o != cv.cv_id],
            )
        log.info("cv matched", extra={"event_id": env.event_id, "cv_id": str(cv.cv_id), "status": cv.status})

    def _score_cv(self, cv: CvProfile, vector: list[float]) -> list[MatchRow]:
        """Scores a parsed CV against the nearest open jobs."""
        if cv.parsed is None:
            return []
        hits = self._vectors.nearest_jobs(vector, POOL)
        jobs = self._store.open_jobs([job_id for job_id, _ in hits])
        rows: list[MatchRow] = []
        for job_id, sim in hits:
            job = jobs.get(job_id)
            if job is None:
                continue
            total, breakdown = score(sim, job, cv.parsed)
            rows.append(
                MatchRow(job_id, cv.cv_id, cv.candidate_id, total, breakdown, self._embedder.model_version)
            )
        return rows

    def _rescore_cv(self, conn: Any, cv_id: UUID) -> None:
        cv = self._store.get_cv(conn, cv_id)
        vector = self._vectors.get_cv_vector(cv_id)
        if cv is None or vector is None:
            # No vector in this model's collection (e.g. after a provider change):
            # the CV is matched again on its next cv.updated.
            return
        self._store.delete_results_for_cvs(conn, [cv_id])
        self._store.upsert_results(conn, self._score_cv(cv, vector))

    def _skip(self, env: Envelope) -> None:
        with self._store.transaction() as conn:
            self._store.record_event(conn, env.event_id)
        log.info("event skipped (duplicate or stale)", extra={"event_id": env.event_id})
