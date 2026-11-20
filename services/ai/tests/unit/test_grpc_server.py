import uuid
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime

import grpc
import pytest

from ai_service.adapters.embedder import FakeEmbedder
from ai_service.adapters.grpc_server import AIServicer, build_server
from ai_service.domain.matching import Breakdown, CvProfile, JobProfile, MatchRow, score
from ai_service.domain.models import ParsedCV
from smarthire.ai.v1 import ai_pb2, ai_pb2_grpc

EMB = FakeEmbedder()
MODEL = EMB.model_version


def _job(title: str, skills: list[str], status: str = "open") -> JobProfile:
    return JobProfile(
        uuid.uuid4(), uuid.uuid4(), 1, status, title, title, skills, [], "middle", "Hà Nội", None, None
    )


def _parsed() -> ParsedCV:
    return ParsedCV(
        full_name=None,
        email=None,
        skills=["Go", "Kafka"],
        years_of_experience=3.0,
        current_title="Golang backend developer",
        education=[],
        experiences=[],
        languages=[],
        location="Hà Nội",
    )


class FakeStore:
    def __init__(self) -> None:
        self.rows: dict[uuid.UUID, list[MatchRow]] = {}
        self.cvs: dict[uuid.UUID, CvProfile] = {}
        self.jobs: dict[uuid.UUID, JobProfile] = {}

    def top_candidates(self, job_id: uuid.UUID, k: int) -> list[MatchRow]:
        return sorted(self.rows.get(job_id, []), key=lambda r: -r.score)[:k]

    def load_cv(self, cv_id: uuid.UUID) -> CvProfile | None:
        return self.cvs.get(cv_id)

    def open_jobs(self, ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, JobProfile]:
        return {i: self.jobs[i] for i in ids if i in self.jobs and self.jobs[i].status == "open"}


class FakeVectors:
    def __init__(self) -> None:
        self.jobs: dict[uuid.UUID, list[float]] = {}
        self.cvs: dict[uuid.UUID, list[float]] = {}

    def get_cv_vector(self, cv_id: uuid.UUID) -> list[float] | None:
        return self.cvs.get(cv_id)

    def get_job_vector(self, job_id: uuid.UUID) -> list[float] | None:
        return self.jobs.get(job_id)

    def nearest_jobs(self, vector: list[float], limit: int) -> list[tuple[uuid.UUID, float]]:
        sims = [(j, sum(a * b for a, b in zip(vector, v, strict=True))) for j, v in self.jobs.items()]
        return sorted(sims, key=lambda x: -x[1])[:limit]


@pytest.fixture
def env() -> Iterator[tuple[ai_pb2_grpc.AIServiceStub, FakeStore, FakeVectors]]:
    store, vectors = FakeStore(), FakeVectors()
    server, port = build_server(AIServicer(store, vectors, MODEL), "127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = ai_pb2_grpc.AIServiceStub(channel)
    stub.channel = channel  # type: ignore[attr-defined]
    try:
        yield stub, store, vectors
    finally:
        channel.close()
        server.stop(0)


def _add_cv(store: FakeStore, vectors: FakeVectors) -> CvProfile:
    cv = CvProfile(uuid.uuid4(), uuid.uuid4(), 2, "PARSED", _parsed(), datetime(2026, 11, 20, tzinfo=UTC))
    store.cvs[cv.cv_id] = cv
    vectors.cvs[cv.cv_id] = EMB.embed(["Golang backend developer Go Kafka"])[0]
    return cv


def _add_job(
    store: FakeStore, vectors: FakeVectors, title: str, skills: list[str], status: str = "open"
) -> JobProfile:
    job = _job(title, skills, status)
    store.jobs[job.job_id] = job
    vectors.jobs[job.job_id] = EMB.embed([title + " " + " ".join(skills)])[0]
    return job


def test_find_candidates_sorted_and_limited(env) -> None:  # type: ignore[no-untyped-def]
    stub, store, _ = env
    job = uuid.uuid4()
    b = Breakdown(0.5, 1.0, 1.0, 1.0, ["Go"], ["Rust"])
    store.rows[job] = [MatchRow(job, uuid.uuid4(), uuid.uuid4(), s, b, MODEL) for s in (40.0, 90.5, 70.0)]
    resp = stub.FindCandidates(ai_pb2.FindCandidatesRequest(job_id=str(job), top_k=2))
    assert [c.score for c in resp.candidates] == [90.5, 70.0]
    assert resp.model_version == MODEL
    assert list(resp.candidates[0].breakdown.matched_skills) == ["Go"]
    assert list(resp.candidates[0].breakdown.missing_skills) == ["Rust"]


def test_find_candidates_unknown_job_empty(env) -> None:  # type: ignore[no-untyped-def]
    stub, _, _ = env
    assert list(stub.FindCandidates(ai_pb2.FindCandidatesRequest(job_id=str(uuid.uuid4()))).candidates) == []


@pytest.mark.parametrize(("asked", "used"), [(0, 20), (-5, 20), (3, 3), (500, 50)])
def test_top_k_clamped(env, asked: int, used: int) -> None:  # type: ignore[no-untyped-def]
    stub, store, _ = env
    job = uuid.uuid4()
    b = Breakdown(0.5, 1.0, 1.0, 1.0, [], [])
    store.rows[job] = [MatchRow(job, uuid.uuid4(), uuid.uuid4(), float(i), b, MODEL) for i in range(60)]
    resp = stub.FindCandidates(ai_pb2.FindCandidatesRequest(job_id=str(job), top_k=asked))
    assert len(resp.candidates) == used


def test_recommend_jobs_only_open_jobs(env) -> None:  # type: ignore[no-untyped-def]
    stub, store, vectors = env
    cv = _add_cv(store, vectors)
    go = _add_job(store, vectors, "Golang backend developer", ["go", "kafka"])
    _add_job(store, vectors, "Golang backend developer", ["go"], status="closed")
    chef = _add_job(store, vectors, "Pastry chef", ["baking"])
    resp = stub.RecommendJobs(ai_pb2.RecommendJobsRequest(cv_id=str(cv.cv_id)))
    assert [j.job_id for j in resp.jobs] == [str(go.job_id), str(chef.job_id)]
    assert resp.jobs[0].score > resp.jobs[1].score
    assert list(resp.jobs[0].breakdown.matched_skills) == ["Go", "Kafka"]


def test_recommend_jobs_cv_without_vector_not_found(env) -> None:  # type: ignore[no-untyped-def]
    stub, store, vectors = env
    cv = _add_cv(store, vectors)
    del vectors.cvs[cv.cv_id]
    with pytest.raises(grpc.RpcError) as err:
        stub.RecommendJobs(ai_pb2.RecommendJobsRequest(cv_id=str(cv.cv_id)))
    assert err.value.code() == grpc.StatusCode.NOT_FOUND
    with pytest.raises(grpc.RpcError) as err:
        stub.RecommendJobs(ai_pb2.RecommendJobsRequest(cv_id=str(uuid.uuid4())))
    assert err.value.code() == grpc.StatusCode.NOT_FOUND


def test_score_match_matches_domain_score(env) -> None:  # type: ignore[no-untyped-def]
    stub, store, vectors = env
    cv = _add_cv(store, vectors)
    job = _add_job(store, vectors, "Golang backend developer", ["go", "rust"])
    resp = stub.ScoreMatch(ai_pb2.ScoreMatchRequest(cv_id=str(cv.cv_id), job_id=str(job.job_id)))
    sim = sum(a * b for a, b in zip(vectors.cvs[cv.cv_id], vectors.jobs[job.job_id], strict=True))
    want, breakdown = score(sim, job, _parsed())
    assert resp.score == pytest.approx(want)
    assert resp.breakdown.required_skill_coverage == pytest.approx(breakdown.required_skill_coverage)
    assert list(resp.breakdown.missing_skills) == ["rust"] or list(resp.breakdown.missing_skills) == ["Rust"]
    assert resp.model_version == MODEL


def test_score_match_closed_job_not_found(env) -> None:  # type: ignore[no-untyped-def]
    stub, store, vectors = env
    cv = _add_cv(store, vectors)
    job = _add_job(store, vectors, "Go", ["go"], status="closed")
    with pytest.raises(grpc.RpcError) as err:
        stub.ScoreMatch(ai_pb2.ScoreMatchRequest(cv_id=str(cv.cv_id), job_id=str(job.job_id)))
    assert err.value.code() == grpc.StatusCode.NOT_FOUND


@pytest.mark.parametrize(
    "call",
    [
        lambda s: s.FindCandidates(ai_pb2.FindCandidatesRequest(job_id="nope")),
        lambda s: s.RecommendJobs(ai_pb2.RecommendJobsRequest(cv_id="")),
        lambda s: s.ScoreMatch(ai_pb2.ScoreMatchRequest(cv_id=str(uuid.uuid4()), job_id="x")),
    ],
)
def test_bad_uuid_invalid_argument(env, call) -> None:  # type: ignore[no-untyped-def]
    stub, _, _ = env
    with pytest.raises(grpc.RpcError) as err:
        call(stub)
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_health_serving(env) -> None:  # type: ignore[no-untyped-def]
    from grpc_health.v1 import health_pb2, health_pb2_grpc

    stub, _, _ = env
    channel = stub.channel
    resp = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest())
    assert resp.status == health_pb2.HealthCheckResponse.SERVING
