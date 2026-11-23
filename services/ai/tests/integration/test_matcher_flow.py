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


# --- account deletion (ADR 0014) ---


def user_deleted(user: uuid.UUID, role: str = "candidate") -> Envelope:
    return env(
        "user.deleted",
        {
            "user_id": str(user),
            "role": role,
            "deletion_request_id": str(uuid.uuid4()),
            "requested_at": NOW.isoformat(),
        },
        source="auth-service",
    )


def _parse_result(engine: Engine, cv: uuid.UUID, candidate: uuid.UUID | None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO cv_parse_results "
                "(cv_id, attempt_id, status, parsed, text_chars, truncated, candidate_id) "
                "VALUES (:cv, 'a1', 'PARSED', CAST(:doc AS jsonb), 10, false, :cand)"
            ),
            {"cv": cv, "cand": candidate, "doc": '{"full_name": "A"}'},
        )
        conn.execute(
            text(
                "INSERT INTO outbox (id, aggregate_id, event_type, payload, published_at) "
                "VALUES (gen_random_uuid(), :key, 'cv.parsed', CAST(:doc AS jsonb), now())"
            ),
            {"key": str(cv), "doc": '{"payload": {"parsed": {"full_name": "A"}}}'},
        )


def _count(engine: Engine, sql: str, **params: Any) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(sql), params).scalar_one())


def test_user_deleted_purges_candidate_and_confirms(setup: Any, engine: Engine) -> None:
    handler, store, vectors = setup
    cand, other = uuid.uuid4(), uuid.uuid4()
    cv, unseen, keep = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    job = uuid.uuid4()
    handler.handle(job_event(job))
    handler.handle(cv_event(cv, cand))
    handler.handle(cv_event(keep, other))
    _parse_result(engine, cv, cand)
    _parse_result(engine, unseen, cand)  # parsed, but the matcher never saw its cv.updated
    _parse_result(engine, keep, other)
    assert {r.cv_id for r in store.top_candidates(job, 10)} == {cv, keep}

    ev = user_deleted(cand)
    handler.handle(ev)
    handler.handle(ev)  # redelivery

    got = store.load_cv(cv)
    assert got.status == "DELETED"
    assert got.parsed is None
    assert vectors.get_cv_vector(cv) is None
    assert [r.cv_id for r in store.top_candidates(job, 10)] == [keep]
    assert _count(engine, "SELECT count(*) FROM match_results WHERE candidate_id = :c", c=cand) == 0
    assert _count(engine, "SELECT count(*) FROM cv_parse_results WHERE candidate_id = :c", c=cand) == 0
    assert _count(engine, "SELECT count(*) FROM cv_parse_results WHERE cv_id = :k", k=keep) == 1
    assert (
        _count(
            engine,
            "SELECT count(*) FROM outbox WHERE event_type = 'cv.parsed' AND aggregate_id IN (:a, :b)",
            a=str(cv),
            b=str(unseen),
        )
        == 0
    ), "published cv.parsed rows held the parsed CV"
    assert vectors.get_cv_vector(keep) is not None

    purged = [e for e in outbox(engine) if e["type"] == "user.data_purged"]
    assert len(purged) == 1
    assert purged[0]["key"] == str(cand)
    assert purged[0]["payload"]["payload"]["service"] == "ai"
    assert purged[0]["payload"]["source"] == "ai-service"

    handler.handle(user_deleted(cand))  # republish: confirm again
    assert len([e for e in outbox(engine) if e["type"] == "user.data_purged"]) == 2


def test_late_cv_updated_after_purge_cannot_revive(setup: Any) -> None:
    handler, store, vectors = setup
    cand, cv = uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(cv, cand, version=2))
    handler.handle(user_deleted(cand))
    handler.handle(cv_event(cv, cand, version=3))  # older than the delete, delivered late
    got = store.load_cv(cv)
    assert got.status == "DELETED"
    assert got.parsed is None
    assert vectors.get_cv_vector(cv) is None


def test_cv_deleted_drops_vector_results_and_parse_results(setup: Any, engine: Engine) -> None:
    handler, store, vectors = setup
    cand, cv, job = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    handler.handle(job_event(job))
    handler.handle(cv_event(cv, cand, version=2))
    _parse_result(engine, cv, None)  # row written before the candidate_id column existed
    handler.handle(cv_event(cv, cand, version=3, status="DELETED"))
    assert store.load_cv(cv).status == "DELETED"
    assert vectors.get_cv_vector(cv) is None
    assert store.top_candidates(job, 10) == []
    assert _count(engine, "SELECT count(*) FROM cv_parse_results WHERE cv_id = :c", c=cv) == 0
    assert (
        _count(
            engine,
            "SELECT count(*) FROM outbox WHERE event_type = 'cv.parsed' AND aggregate_id = :c",
            c=str(cv),
        )
        == 0
    )


def test_user_deleted_anonymises_recruiter_jobs(setup: Any, engine: Engine) -> None:
    handler, store, _ = setup
    job = uuid.uuid4()
    handler.handle(job_event(job))
    handler.handle(user_deleted(RECRUITER, role="recruiter"))
    assert store.load_job(job).recruiter_id == uuid.UUID(int=0)


def test_user_deleted_from_wrong_source_is_permanent(setup: Any) -> None:
    handler, _, _ = setup
    bad = user_deleted(uuid.uuid4())
    bad = bad.model_copy(update={"source": "job-service"})
    with pytest.raises(PermanentError):
        handler.handle(bad)


def test_worker_does_not_store_results_for_a_tombstoned_cv(setup: Any, engine: Engine) -> None:
    from ai_service.adapters.db import PgResultStore
    from ai_service.domain.events import CvUploadedPayload, result_envelope
    from ai_service.domain.models import ParsedCV, ParseSuccess

    handler, _, _ = setup
    cand, cv = uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(cv, cand, version=2))
    handler.handle(user_deleted(cand))

    payload = CvUploadedPayload(
        cv_id=cv, candidate_id=cand, object_key=f"cvs/{cand}/{cv}.pdf", attempt_id=str(ULID())
    )
    outcome = ParseSuccess(
        parsed=ParsedCV.model_validate(parsed(["Go"])), model_version="fake", text_chars=10, truncated=False
    )
    result = result_envelope(outcome, payload, now=NOW, trace_id="", correlation_id="")
    assert PgResultStore(engine).commit_outcome(str(ULID()), payload, outcome, result)
    assert _count(engine, "SELECT count(*) FROM cv_parse_results WHERE cv_id = :c", c=cv) == 0
    assert (
        _count(
            engine,
            "SELECT count(*) FROM outbox WHERE event_type = 'cv.parsed' AND aggregate_id = :c",
            c=str(cv),
        )
        == 0
    )

    other_cv = uuid.uuid4()
    payload2 = CvUploadedPayload(
        cv_id=other_cv, candidate_id=cand, object_key=f"cvs/{cand}/{other_cv}.pdf", attempt_id=str(ULID())
    )
    result2 = result_envelope(outcome, payload2, now=NOW, trace_id="", correlation_id="")
    assert PgResultStore(engine).commit_outcome(str(ULID()), payload2, outcome, result2)
    assert (
        _count(
            engine,
            "SELECT count(*) FROM cv_parse_results WHERE cv_id = :c AND candidate_id = :d",
            c=other_cv,
            d=cand,
        )
        == 1
    ), "results keep the candidate for a later purge"


def test_only_cv_reparsed_leaves_no_active_cv(setup: Any) -> None:
    # Regression: with no PARSED CV left, refresh_active wrote NULL into active.
    handler, store, vectors = setup
    cand, cv, job = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    handler.handle(job_event(job))
    handler.handle(cv_event(cv, cand, version=2))
    handler.handle(cv_event(cv, cand, version=3, status="UPLOADED"))
    assert store.load_cv(cv).status == "UPLOADED"
    assert store.top_candidates(job, 10) == []
    assert vectors.get_cv_vector(cv) is None
