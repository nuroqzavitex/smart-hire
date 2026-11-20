"""AIService gRPC server (smarthire.ai.v1), compose-internal without JWT (ADR 0010).

Job and Candidate check ownership before calling it. FindCandidates reads the
precomputed match_results; RecommendJobs and ScoreMatch score on demand from the
stored vectors and replicas (no embedding call, so no Gemini dependency here).
"""

import logging
from collections.abc import Sequence
from concurrent import futures
from typing import Any, Protocol
from uuid import UUID

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from opentelemetry.instrumentation.grpc import server_interceptor

from ai_service.domain.matching import Breakdown, CvProfile, JobProfile, MatchRow, score
from ai_service.domain.models import ParsedCV
from smarthire.ai.v1 import ai_pb2, ai_pb2_grpc

log = logging.getLogger(__name__)

DEFAULT_TOP_K = 20
MAX_TOP_K = 50
POOL = 50
MAX_WORKERS = 8


class MatchReader(Protocol):
    def top_candidates(self, job_id: UUID, k: int) -> list[MatchRow]: ...
    def load_cv(self, cv_id: UUID) -> CvProfile | None: ...
    def open_jobs(self, job_ids: Sequence[UUID]) -> dict[UUID, JobProfile]: ...


class VectorReader(Protocol):
    def get_cv_vector(self, cv_id: UUID) -> list[float] | None: ...
    def get_job_vector(self, job_id: UUID) -> list[float] | None: ...
    def nearest_jobs(self, vector: list[float], limit: int) -> list[tuple[UUID, float]]: ...


class _Abort(Exception):
    def __init__(self, code: grpc.StatusCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _uuid(value: str, field: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise _Abort(grpc.StatusCode.INVALID_ARGUMENT, f"{field} must be a UUID") from None


def _top_k(value: int) -> int:
    if value <= 0:
        return DEFAULT_TOP_K
    return min(value, MAX_TOP_K)


def _breakdown(b: Breakdown) -> ai_pb2.ScoreBreakdown:
    return ai_pb2.ScoreBreakdown(
        semantic_similarity=b.semantic_similarity,
        required_skill_coverage=b.required_skill_coverage,
        experience_fit=b.experience_fit,
        location_fit=b.location_fit,
        matched_skills=b.matched_skills,
        missing_skills=b.missing_skills,
    )


def _dot(a: list[float], b: list[float]) -> float:
    # Stored vectors are L2-normalised, so the dot product is the cosine.
    return sum(x * y for x, y in zip(a, b, strict=True))


class AIServicer(ai_pb2_grpc.AIServiceServicer):
    def __init__(self, store: MatchReader, vectors: VectorReader, model_version: str) -> None:
        self._store = store
        self._vectors = vectors
        self._model_version = model_version

    def FindCandidates(self, request: Any, context: grpc.ServicerContext) -> Any:
        return self._guard(context, lambda: self._find(request))

    def RecommendJobs(self, request: Any, context: grpc.ServicerContext) -> Any:
        return self._guard(context, lambda: self._recommend(request))

    def ScoreMatch(self, request: Any, context: grpc.ServicerContext) -> Any:
        return self._guard(context, lambda: self._score(request))

    def _guard(self, context: grpc.ServicerContext, fn: Any) -> Any:
        try:
            return fn()
        except _Abort as exc:
            context.abort(exc.code, exc.message)
        except Exception as exc:
            # Type only: messages may echo SQL parameters.
            log.error("grpc handler failed", extra={"error_type": type(exc).__name__})
            context.abort(grpc.StatusCode.UNAVAILABLE, "matching temporarily unavailable")

    def _find(self, req: ai_pb2.FindCandidatesRequest) -> ai_pb2.FindCandidatesResponse:
        job_id = _uuid(req.job_id, "job_id")
        rows = self._store.top_candidates(job_id, _top_k(req.top_k))
        return ai_pb2.FindCandidatesResponse(
            candidates=[
                ai_pb2.RankedCandidate(
                    candidate_id=str(r.candidate_id),
                    cv_id=str(r.cv_id),
                    score=r.score,
                    breakdown=_breakdown(r.breakdown),
                )
                for r in rows
            ],
            model_version=self._model_version,
        )

    def _parsed_cv(self, cv_id: UUID) -> tuple[ParsedCV, list[float]]:
        cv = self._store.load_cv(cv_id)
        vector = self._vectors.get_cv_vector(cv_id)
        if cv is None or cv.parsed is None or vector is None:
            raise _Abort(grpc.StatusCode.NOT_FOUND, "cv not matched yet")
        return cv.parsed, vector

    def _recommend(self, req: ai_pb2.RecommendJobsRequest) -> ai_pb2.RecommendJobsResponse:
        parsed, vector = self._parsed_cv(_uuid(req.cv_id, "cv_id"))
        hits = self._vectors.nearest_jobs(vector, POOL)
        jobs = self._store.open_jobs([job_id for job_id, _ in hits])
        ranked: list[tuple[float, UUID, Breakdown]] = []
        for job_id, sim in hits:
            job = jobs.get(job_id)
            if job is None:
                continue
            total, breakdown = score(sim, job, parsed)
            ranked.append((total, job_id, breakdown))
        ranked.sort(key=lambda r: (-r[0], str(r[1])))
        return ai_pb2.RecommendJobsResponse(
            jobs=[
                ai_pb2.RankedJob(job_id=str(job_id), score=total, breakdown=_breakdown(b))
                for total, job_id, b in ranked[: _top_k(req.top_k)]
            ],
            model_version=self._model_version,
        )

    def _score(self, req: ai_pb2.ScoreMatchRequest) -> ai_pb2.ScoreMatchResponse:
        cv_id = _uuid(req.cv_id, "cv_id")
        job_id = _uuid(req.job_id, "job_id")
        parsed, cv_vector = self._parsed_cv(cv_id)
        job = self._store.open_jobs([job_id]).get(job_id)
        job_vector = self._vectors.get_job_vector(job_id)
        if job is None or job_vector is None:
            raise _Abort(grpc.StatusCode.NOT_FOUND, "job not matched yet")
        total, breakdown = score(_dot(cv_vector, job_vector), job, parsed)
        return ai_pb2.ScoreMatchResponse(
            score=total, breakdown=_breakdown(breakdown), model_version=self._model_version
        )


def build_server(servicer: AIServicer, address: str) -> tuple[grpc.Server, int]:
    """Server with the AIService, the standard health service and OTel tracing; returns the bound port."""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=MAX_WORKERS),
        interceptors=[server_interceptor()],  # type: ignore[no-untyped-call]
    )
    ai_pb2_grpc.add_AIServiceServicer_to_server(servicer, server)  # type: ignore[no-untyped-call]
    health_servicer = health.HealthServicer()
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    port = server.add_insecure_port(address)
    return server, port
