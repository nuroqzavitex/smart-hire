import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from qdrant_client import QdrantClient
from sqlalchemy import Engine, text
from ulid import ULID

from ai_service.adapters.embedder import FakeEmbedder
from ai_service.adapters.match_store import PgMatchStore
from ai_service.adapters.vectors import QdrantVectors, collection_names
from ai_service.domain.events import Envelope
from ai_service.services.cv_uploaded_handler import PermanentError
from ai_service.services.embedding import EmbeddingUnavailable
from ai_service.services.matcher import MatchHandler

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_matching")]

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
RECRUITER = uuid.UUID("11111111-1111-4111-8111-111111111111")


class FlakyEmbedder(FakeEmbedder):
    def __init__(self) -> None:
        self.fail = 0

    def embed(self, texts: Any) -> list[list[float]]:
        if self.fail:
            self.fail -= 1
            raise EmbeddingUnavailable("gemini status 429")
        return super().embed(texts)


@pytest.fixture
def setup(engine: Engine, qdrant_url: str) -> Iterator[tuple[MatchHandler, PgMatchStore, QdrantVectors]]:
    client = QdrantClient(url=qdrant_url, timeout=5)
    emb = FlakyEmbedder()
    version = f"flow-{uuid.uuid4().hex[:8]}@768"
    emb.model_version = version  # type: ignore[misc]
    vectors = QdrantVectors(client, version)
    vectors.ensure_collections()
    store = PgMatchStore(engine)
    handler = MatchHandler(store, vectors, emb, clock=lambda: NOW, trace_id=lambda: "t")
    yield handler, store, vectors
    for name in collection_names(version):
        client.delete_collection(name)


def env(event_type: str, payload: dict[str, Any], source: str | None = None) -> Envelope:
    src = source or ("candidate-service" if event_type == "cv.updated" else "job-service")
    return Envelope(
        event_id=str(ULID()),
        event_type=event_type,
        event_version=1,
        source=src,
        occurred_at=NOW,
        trace_id="t",
        correlation_id="c",
        payload=payload,
    )


def job_event(
    job_id: uuid.UUID,
    *,
    version: int = 1,
    status: str = "open",
    skills: list[str] | None = None,
    event_type: str = "job.created",
) -> Envelope:
    return env(
        event_type,
        {
            "job_id": str(job_id),
            "recruiter_id": str(RECRUITER),
            "title": "Golang backend engineer",
            "description": "Build golang services with kafka and postgres",
            "location": "Hà Nội",
            "required_skills": skills if skills is not None else ["go", "kafka"],
            "nice_to_have_skills": [],
            "level": "middle",
            "salary_min": None,
            "salary_max": 3000,
            "status": status,
            "version": version,
            "published_at": NOW.isoformat() if status == "open" else None,
        },
    )


def parsed(skills: list[str], title: str = "Golang backend developer") -> dict[str, Any]:
    return {
        "full_name": "Nguyễn Văn A",
        "email": None,
        "skills": skills,
        "years_of_experience": 3.0,
        "current_title": title,
        "education": [],
        "experiences": [],
        "languages": [],
        "location": "Hà Nội",
    }


def cv_event(
    cv_id: uuid.UUID,
    candidate: uuid.UUID,
    *,
    version: int = 2,
    status: str = "PARSED",
    skills: list[str] | None = None,
    at: datetime = NOW,
    title: str = "Golang backend developer",
) -> Envelope:
    is_parsed = status == "PARSED"
    return env(
        "cv.updated",
        {
            "cv_id": str(cv_id),
            "candidate_id": str(candidate),
            "version": version,
            "status": status,
            "parsed": parsed(skills or ["Go", "Kafka"], title) if is_parsed else None,
            "parsed_at": at.isoformat() if is_parsed else None,
        },
    )


def outbox(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT event_type, aggregate_id, payload FROM outbox ORDER BY created_at")
        ).all()
    return [{"type": r.event_type, "key": r.aggregate_id, "payload": r.payload} for r in rows]


def test_job_created_scores_active_cvs_and_emits_completed(setup: Any, engine: Engine) -> None:
    handler, store, _ = setup
    go_cv, java_cv = uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(go_cv, uuid.uuid4()))
    handler.handle(cv_event(java_cv, uuid.uuid4(), skills=["Java"], title="Java spring developer"))
    job = uuid.uuid4()
    handler.handle(job_event(job))

    top = store.top_candidates(job, 10)
    assert [r.cv_id for r in top] == [go_cv, java_cv]
    assert top[0].score > top[1].score
    assert top[0].breakdown.matched_skills == ["Go", "Kafka"]
    assert top[1].score <= 40.0, "missing all required skills caps the score"
    events = [e for e in outbox(engine) if e["type"] == "match.completed"]
    assert len(events) == 1
    payload = json.loads(json.dumps(events[0]["payload"]))["payload"]
    assert events[0]["key"] == str(job)
    assert [t["cv_id"] for t in payload["top"]] == [str(go_cv), str(java_cv)]


def test_cv_updated_parsed_upserts_results(setup: Any) -> None:
    handler, store, _ = setup
    job = uuid.uuid4()
    handler.handle(job_event(job))
    assert store.top_candidates(job, 10) == []
    cv = uuid.uuid4()
    handler.handle(cv_event(cv, uuid.uuid4()))
    assert [r.cv_id for r in store.top_candidates(job, 10)] == [cv]


def test_cv_updated_older_version_ignored(setup: Any) -> None:
    handler, store, vectors = setup
    cv, cand = uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(cv, cand, version=5))
    handler.handle(cv_event(cv, cand, version=4, status="UPLOADED"))
    got = store.load_cv(cv)
    assert got.version == 5
    assert got.status == "PARSED"
    assert vectors.get_cv_vector(cv) is not None


def test_active_cv_switches_on_reparse(setup: Any) -> None:
    handler, store, _ = setup
    job = uuid.uuid4()
    handler.handle(job_event(job))
    cand = uuid.uuid4()
    old, new = uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(old, cand, at=NOW))
    handler.handle(cv_event(new, cand, at=NOW + timedelta(hours=1)))
    assert [r.cv_id for r in store.top_candidates(job, 10)] == [new]
    handler.handle(cv_event(new, cand, version=3, status="UPLOADED"))
    assert [r.cv_id for r in store.top_candidates(job, 10)] == [old]


def test_reactivated_cv_rescored_for_jobs_matched_while_inactive(setup: Any) -> None:
    handler, store, _ = setup
    cand = uuid.uuid4()
    old, new = uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(old, cand, at=NOW))
    handler.handle(cv_event(new, cand, at=NOW + timedelta(hours=1)))
    job = uuid.uuid4()
    handler.handle(job_event(job))  # matched while `old` is inactive: only `new` is scored
    assert [r.cv_id for r in store.top_candidates(job, 10)] == [new]
    handler.handle(cv_event(new, cand, version=3, status="UPLOADED"))
    assert [r.cv_id for r in store.top_candidates(job, 10)] == [old]


def test_reactivating_cv_without_vector_does_not_fail(setup: Any, engine: Engine) -> None:
    # After a provider change the new collections are empty, while match_cvs
    # still holds PARSED rows from the old model.
    handler, store, vectors = setup
    cand = uuid.uuid4()
    old, new = uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(old, cand, at=NOW))
    vectors.delete_cv(old)
    handler.handle(cv_event(new, cand, at=NOW + timedelta(hours=1)))
    handler.handle(cv_event(new, cand, version=3, status="UPLOADED"))
    assert store.load_cv(new).status == "UPLOADED"


def test_job_closed_removes_vector_and_results(setup: Any) -> None:
    handler, store, vectors = setup
    job = uuid.uuid4()
    handler.handle(cv_event(uuid.uuid4(), uuid.uuid4()))
    handler.handle(job_event(job))
    assert store.top_candidates(job, 10)
    handler.handle(env("job.closed", {"job_id": str(job), "version": 2}))
    assert store.top_candidates(job, 10) == []
    assert vectors.get_job_vector(job) is None
    assert store.load_job(job).status == "closed"
    handler.handle(job_event(job, version=1, event_type="job.updated"))
    assert store.load_job(job).status == "closed", "older update does not reopen"


def test_job_updated_replaces_results(setup: Any) -> None:
    handler, store, _ = setup
    go_cv, rust_cv = uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(go_cv, uuid.uuid4()))
    handler.handle(cv_event(rust_cv, uuid.uuid4(), skills=["Rust"], title="Rust systems developer"))
    job = uuid.uuid4()
    handler.handle(job_event(job))
    before = {r.cv_id: r.score for r in store.top_candidates(job, 10)}
    handler.handle(job_event(job, version=2, skills=["rust"], event_type="job.updated"))
    after = {r.cv_id: r for r in store.top_candidates(job, 10)}
    assert after[rust_cv].breakdown.matched_skills == ["rust"] or after[rust_cv].breakdown.matched_skills == [
        "Rust"
    ]
    assert after[go_cv].breakdown.missing_skills
    assert after[go_cv].score != before[go_cv]


def test_draft_job_not_matched(setup: Any) -> None:
    handler, store, vectors = setup
    handler.handle(cv_event(uuid.uuid4(), uuid.uuid4()))
    job = uuid.uuid4()
    handler.handle(job_event(job, status="draft"))
    assert store.top_candidates(job, 10) == []
    assert vectors.get_job_vector(job) is None
    assert store.load_job(job).status == "draft"


def test_duplicate_event_is_noop(setup: Any, engine: Engine) -> None:
    handler, _, _ = setup
    e = job_event(uuid.uuid4())
    handler.handle(e)
    handler.handle(e)
    assert len([x for x in outbox(engine) if x["type"] == "match.completed"]) == 1


def test_transient_embedding_error_leaves_nothing_recorded(setup: Any, engine: Engine) -> None:
    handler, store, _ = setup
    handler._embedder.fail = 1
    e = job_event(uuid.uuid4())
    with pytest.raises(EmbeddingUnavailable):
        handler.handle(e)
    handler.handle(e)
    assert store.load_job(uuid.UUID(e.payload["job_id"])) is not None


@pytest.mark.parametrize(
    "bad",
    [
        lambda: env("job.created", {"job_id": "nope"}),
        lambda: env("cv.updated", {"cv_id": str(uuid.uuid4())}),
        lambda: env("job.created", job_event(uuid.uuid4()).payload, source="candidate-service"),
        lambda: env("cv.updated", cv_event(uuid.uuid4(), uuid.uuid4()).payload, source="job-service"),
        lambda: env("cv.updated", {**cv_event(uuid.uuid4(), uuid.uuid4()).payload, "parsed": None}),
        lambda: env("cv.parsed", {}),
    ],
)
def test_bad_payload_is_permanent(setup: Any, bad: Any) -> None:
    handler, _, _ = setup
    with pytest.raises(PermanentError):
        handler.handle(bad())


def test_scores_are_recorded_as_metrics(engine: Engine, qdrant_url: str) -> None:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    from ai_service.metrics import Instruments

    reader = InMemoryMetricReader()
    client = QdrantClient(url=qdrant_url, timeout=5)
    emb = FlakyEmbedder()
    version = f"flow-{uuid.uuid4().hex[:8]}@768"
    emb.model_version = version  # type: ignore[misc]
    vectors = QdrantVectors(client, version)
    vectors.ensure_collections()
    try:
        handler = MatchHandler(
            PgMatchStore(engine),
            vectors,
            emb,
            clock=lambda: NOW,
            trace_id=lambda: "t",
            instruments=Instruments(MeterProvider(metric_readers=[reader])),
        )
        handler.handle(job_event(uuid.uuid4()))
        handler.handle(cv_event(uuid.uuid4(), uuid.uuid4()))
        data = reader.get_metrics_data()
        assert data is not None
        counts = [
            p.count
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for m in sm.metrics
            if m.name == "ai.match.score"
            for p in m.data.data_points
        ]
        assert counts == [1], "the CV scored against the one open job"
    finally:
        for name in collection_names(version):
            client.delete_collection(name)
