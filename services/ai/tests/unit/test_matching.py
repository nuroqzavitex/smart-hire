import uuid

import pytest

from ai_service.domain.matching import JobProfile, cv_text, job_text, score
from ai_service.domain.models import Education, Experience, ParsedCV


def _job(**kw: object) -> JobProfile:
    base: dict[str, object] = {
        "job_id": uuid.uuid4(),
        "recruiter_id": uuid.uuid4(),
        "version": 1,
        "status": "open",
        "title": "Backend Engineer",
        "description": "Build services in Go",
        "required_skills": ["go", "postgres", "kafka"],
        "nice_to_have_skills": ["docker"],
        "level": "middle",
        "location": "Hà Nội",
        "salary_min": None,
        "salary_max": None,
    }
    base.update(kw)
    return JobProfile(**base)  # type: ignore[arg-type]


def _cv(**kw: object) -> ParsedCV:
    base: dict[str, object] = {
        "full_name": None,
        "email": None,
        "skills": ["Golang", "PostgreSQL", "Kafka"],
        "years_of_experience": 3.0,
        "current_title": "Backend developer",
        "education": [],
        "experiences": [],
        "languages": [],
        "location": "ha noi",
    }
    base.update(kw)
    return ParsedCV.model_validate(base)


def test_perfect_match_is_100() -> None:
    total, b = score(1.0, _job(), _cv())
    assert total == 100.0
    assert b.required_skill_coverage == 1.0
    assert b.matched_skills == ["Go", "PostgreSQL", "Kafka"]
    assert b.missing_skills == []


def test_low_coverage_is_capped_at_40() -> None:
    total, b = score(0.9, _job(), _cv(skills=["Go"]))
    assert b.required_skill_coverage == pytest.approx(1 / 3)
    assert total == 40.0
    assert b.missing_skills == ["PostgreSQL", "Kafka"]


def test_half_coverage_is_not_capped() -> None:
    total, _ = score(1.0, _job(required_skills=["go", "rust"]), _cv())
    assert total == pytest.approx(100 * (0.55 + 0.30 * 0.5 + 0.10 + 0.05))


def test_no_required_skills_full_coverage() -> None:
    _, b = score(0.5, _job(required_skills=[]), _cv(skills=[]))
    assert b.required_skill_coverage == 1.0


def test_synonym_matches() -> None:
    _, b = score(0.5, _job(required_skills=["go"]), _cv(skills=["Golang"]))
    assert b.matched_skills == ["Go"]


@pytest.mark.parametrize(
    ("level", "years", "fit"),
    [
        ("middle", None, 0.5),
        ("middle", 1.0, 0.5),
        ("middle", 2.0, 1.0),
        ("senior", 10.0, 1.0),
        ("lead", 3.5, 0.5),
        ("junior", 0.0, 1.0),
        ("intern", None, 0.5),
    ],
)
def test_experience_fit(level: str, years: float | None, fit: float) -> None:
    _, b = score(0.5, _job(level=level), _cv(years_of_experience=years))
    assert b.experience_fit == pytest.approx(fit)


@pytest.mark.parametrize(
    ("job_loc", "cv_loc", "fit"),
    [
        ("Hà Nội", "ha noi", 1.0),
        ("Hà Nội", "Quận 1, Hà Nội", 1.0),
        ("Hà Nội", None, 0.5),
        (None, "Huế", 0.5),
        ("Hà Nội", "Huế", 0.0),
    ],
)
def test_location_fit(job_loc: str | None, cv_loc: str | None, fit: float) -> None:
    _, b = score(0.5, _job(location=job_loc), _cv(location=cv_loc))
    assert b.location_fit == fit


def test_negative_cosine_is_zero() -> None:
    _, b = score(-0.3, _job(), _cv())
    assert b.semantic_similarity == 0.0


def test_score_rounded_to_two_decimals() -> None:
    total, _ = score(0.123456, _job(), _cv())
    assert total == round(total, 2)


def test_cv_text_excludes_identity() -> None:
    cv = _cv(
        full_name="Nguyễn Văn A",
        email="a@example.com",
        education=[
            Education(school="Đại học Bách Khoa", degree="BSc", major="Computer Science", end_year=2020)
        ],
        experiences=[
            Experience(
                company="Acme Corp",
                title="Go developer",
                start=None,
                end=None,
                highlights=["Built payment APIs"],
            )
        ],
        location="Hà Nội",
    )
    text = cv_text(cv)
    for secret in ("Nguyễn Văn A", "a@example.com", "Bách Khoa", "Acme", "Hà Nội"):
        assert secret not in text
    for kept in ("Backend developer", "Golang", "Go developer", "Built payment APIs", "Computer Science"):
        assert kept in text


def test_job_text_truncated() -> None:
    text = job_text(_job(description="x" * 10000))
    assert len(text) <= 6000
    assert text.startswith("Backend Engineer")
