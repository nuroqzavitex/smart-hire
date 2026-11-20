"""CV <-> job match scoring (spec §5.7) and the text each side is embedded from (ADR 0010).

Pure functions: no I/O. Identity fields (name, email, school, employer, location)
never enter the embedded text: they do not help matching and add bias (spec R3).
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from ai_service.domain.models import ParsedCV
from ai_service.domain.skills import normalize, unaccent

W_SEMANTIC, W_SKILLS, W_EXPERIENCE, W_LOCATION = 0.55, 0.30, 0.10, 0.05
LOW_COVERAGE = 0.5
LOW_COVERAGE_CAP = 40.0
UNKNOWN_FIT = 0.5
MAX_JOB_TEXT = 6000

# Minimum years of experience expected for each job level.
MIN_YEARS: dict[str, int] = {"intern": 0, "junior": 0, "middle": 2, "senior": 5, "lead": 7}


@dataclass(frozen=True)
class JobProfile:
    job_id: UUID
    recruiter_id: UUID
    version: int
    status: str
    title: str
    description: str
    required_skills: list[str]
    nice_to_have_skills: list[str]
    level: str
    location: str | None
    salary_min: int | None
    salary_max: int | None


@dataclass(frozen=True)
class CvProfile:
    cv_id: UUID
    candidate_id: UUID
    version: int
    status: str
    parsed: ParsedCV | None
    parsed_at: datetime | None


@dataclass(frozen=True)
class Breakdown:
    semantic_similarity: float
    required_skill_coverage: float
    experience_fit: float
    location_fit: float
    matched_skills: list[str]
    missing_skills: list[str]


def _fold(text: str) -> str:
    return " ".join(unaccent(text).casefold().split())


def _coverage(required: list[str], have: list[str]) -> tuple[float, list[str], list[str]]:
    wanted = normalize(required)
    if not wanted:
        return 1.0, [], []
    owned = {s.casefold() for s in normalize(have)}
    matched = [s for s in wanted if s.casefold() in owned]
    missing = [s for s in wanted if s.casefold() not in owned]
    return len(matched) / len(wanted), matched, missing


def _experience_fit(level: str, years: float | None) -> float:
    if years is None:
        return UNKNOWN_FIT
    need = MIN_YEARS.get(level, 0)
    if need == 0:
        return 1.0
    return min(1.0, years / need)


def _location_fit(job_location: str | None, cv_location: str | None) -> float:
    a, b = _fold(job_location or ""), _fold(cv_location or "")
    if not a or not b:
        return UNKNOWN_FIT
    return 1.0 if a in b or b in a else 0.0


def score(semantic: float, job: JobProfile, cv: ParsedCV) -> tuple[float, Breakdown]:
    """Returns the 0-100 score (2 decimals) and its breakdown; semantic is the cosine similarity."""
    sem = max(0.0, min(1.0, semantic))
    coverage, matched, missing = _coverage(job.required_skills, cv.skills)
    exp = _experience_fit(job.level, cv.years_of_experience)
    loc = _location_fit(job.location, cv.location)
    total = 100 * (W_SEMANTIC * sem + W_SKILLS * coverage + W_EXPERIENCE * exp + W_LOCATION * loc)
    if coverage < LOW_COVERAGE:
        total = min(total, LOW_COVERAGE_CAP)
    breakdown = Breakdown(sem, coverage, exp, loc, matched, missing)
    return round(total, 2), breakdown


def job_text(job: JobProfile) -> str:
    """Text a job is embedded from: title, level, skills, then the description (truncated)."""
    parts = [
        job.title,
        f"Level: {job.level}",
        "Required skills: " + ", ".join(job.required_skills),
        "Nice to have: " + ", ".join(job.nice_to_have_skills),
        job.description,
    ]
    return "\n".join(p for p in parts if p.strip())[:MAX_JOB_TEXT]


def cv_text(cv: ParsedCV) -> str:
    """Text a CV is embedded from, without identity fields."""
    parts: list[str] = []
    if cv.current_title:
        parts.append(cv.current_title)
    if cv.skills:
        parts.append("Skills: " + ", ".join(cv.skills))
    if cv.years_of_experience is not None:
        parts.append(f"Experience: {cv.years_of_experience:g} years")
    for exp in cv.experiences:
        parts.append(" ".join([exp.title, *exp.highlights]))
    for edu in cv.education:
        study = " ".join(p for p in (edu.degree, edu.major) if p)
        if study:
            parts.append(study)
    if cv.languages:
        parts.append("Languages: " + ", ".join(cv.languages))
    return "\n".join(parts)
