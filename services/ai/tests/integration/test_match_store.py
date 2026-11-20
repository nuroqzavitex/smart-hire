import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine

from ai_service.adapters.match_store import PgMatchStore
from ai_service.domain.matching import Breakdown, CvProfile, JobProfile, MatchRow
from ai_service.domain.models import ParsedCV

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_matching")]

T0 = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)


def job(
    version: int = 1, status: str = "open", title: str = "Go dev", job_id: uuid.UUID | None = None
) -> JobProfile:
    return JobProfile(
        job_id=job_id or uuid.uuid4(),
        recruiter_id=uuid.UUID(int=7),
        version=version,
        status=status,
        title=title,
        description="d",
        required_skills=["go"],
        nice_to_have_skills=[],
        level="middle",
        location="Hà Nội",
        salary_min=None,
        salary_max=1000,
    )


def parsed() -> ParsedCV:
    return ParsedCV(
        full_name=None,
        email=None,
        skills=["Go"],
        years_of_experience=2.0,
        current_title="Dev",
        education=[],
        experiences=[],
        languages=[],
        location=None,
    )


def cv(
    candidate: uuid.UUID,
    version: int = 1,
    status: str = "PARSED",
    at: datetime = T0,
    cv_id: uuid.UUID | None = None,
) -> CvProfile:
    return CvProfile(
        cv_id=cv_id or uuid.uuid4(),
        candidate_id=candidate,
        version=version,
        status=status,
        parsed=parsed() if status == "PARSED" else None,
        parsed_at=at if status == "PARSED" else None,
    )


def row(j: uuid.UUID, c: CvProfile, score: float) -> MatchRow:
    return MatchRow(
        job_id=j,
        cv_id=c.cv_id,
        candidate_id=c.candidate_id,
        score=score,
        breakdown=Breakdown(0.5, 1.0, 1.0, 0.5, ["Go"], []),
        model_version="fake-hash@768",
    )


def test_upsert_job_older_version_ignored(engine: Engine) -> None:
    store = PgMatchStore(engine)
    j = job(version=3, title="v3")
    with engine.begin() as conn:
        assert store.upsert_job(conn, j)
        assert not store.upsert_job(conn, job(version=2, title="v2", job_id=j.job_id))
        assert not store.upsert_job(conn, j), "same version is a no-op"
    assert store.load_job(j.job_id) == j


def test_upsert_cv_round_trip_and_version_guard(engine: Engine) -> None:
    store = PgMatchStore(engine)
    c = cv(uuid.uuid4(), version=2)
    with engine.begin() as conn:
        assert store.upsert_cv(conn, c)
        assert not store.upsert_cv(conn, cv(c.candidate_id, version=1, status="UPLOADED", cv_id=c.cv_id))
    assert store.load_cv(c.cv_id) == c


def test_active_is_newest_parsed_per_candidate(engine: Engine) -> None:
    store = PgMatchStore(engine)
    cand = uuid.uuid4()
    old, new = cv(cand, at=T0), cv(cand, at=T0 + timedelta(hours=1))
    failed = cv(cand, status="FAILED")
    with engine.begin() as conn:
        for c in (old, new, failed):
            store.upsert_cv(conn, c)
        active, inactive = store.refresh_active(conn, cand)
    assert active == new.cv_id
    assert inactive == [old.cv_id]
    with engine.begin() as conn:
        store.upsert_cv(conn, cv(cand, version=2, status="UPLOADED", cv_id=new.cv_id))
        active, inactive = store.refresh_active(conn, cand)
    assert active == old.cv_id
    assert inactive == []


def test_replace_results_for_job_and_top_candidates(engine: Engine) -> None:
    store = PgMatchStore(engine)
    j = job()
    a, b, c = cv(uuid.uuid4()), cv(uuid.uuid4()), cv(uuid.uuid4())
    with engine.begin() as conn:
        store.upsert_job(conn, j)
        for x in (a, b, c):
            store.upsert_cv(conn, x)
            store.refresh_active(conn, x.candidate_id)
        store.replace_job_results(conn, j.job_id, [row(j.job_id, a, 50), row(j.job_id, b, 90)])
        store.replace_job_results(conn, j.job_id, [row(j.job_id, b, 80), row(j.job_id, c, 70)])
    top = store.top_candidates(j.job_id, 10)
    assert [(r.cv_id, r.score) for r in top] == [(b.cv_id, 80.0), (c.cv_id, 70.0)]
    assert top[0].breakdown.matched_skills == ["Go"]
    assert store.top_candidates(j.job_id, 1)[0].cv_id == b.cv_id


def test_top_candidates_skips_inactive_and_deleted(engine: Engine) -> None:
    store = PgMatchStore(engine)
    j = job()
    cand = uuid.uuid4()
    old, new = cv(cand, at=T0), cv(cand, at=T0 + timedelta(hours=1))
    with engine.begin() as conn:
        store.upsert_cv(conn, old)
        store.upsert_cv(conn, new)
        store.refresh_active(conn, cand)
        store.upsert_results(conn, [row(j.job_id, old, 90), row(j.job_id, new, 60)])
    assert [r.cv_id for r in store.top_candidates(j.job_id, 10)] == [new.cv_id]
    with engine.begin() as conn:
        store.delete_results_for_cvs(conn, [new.cv_id])
        store.delete_results_for_job(conn, uuid.uuid4())
    assert store.top_candidates(j.job_id, 10) == []


def test_record_event_once(engine: Engine) -> None:
    store = PgMatchStore(engine)
    with engine.begin() as conn:
        assert store.record_event(conn, "01JC0000000000000000000000")
        assert not store.record_event(conn, "01JC0000000000000000000000")


def test_open_jobs_returns_only_open(engine: Engine) -> None:
    store = PgMatchStore(engine)
    o, c = job(), job(status="closed")
    with engine.begin() as conn:
        store.upsert_job(conn, o)
        store.upsert_job(conn, c)
    assert set(store.open_jobs([o.job_id, c.job_id, uuid.uuid4()])) == {o.job_id}
