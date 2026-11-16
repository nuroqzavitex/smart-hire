"""ParsedCV (spec §5.3, plan decision #2) and the outcome of one parse attempt.

ParsedCV mirrors $defs.ParsedCV in contracts/events/cv.parsed.v1.json; a unit
test keeps the two in lock-step. The *Draft models carry the same fields
without limits: they are what the LLM returns (structured outputs do not
guarantee length/range constraints), and sanitize() clamps them into a ParsedCV.
"""

import re
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from ai_service.domain.skills import MAX_SKILL_LEN, MAX_SKILLS, normalize

MAX_TEXT = 200
MAX_EMAIL = 254
MAX_EDUCATION = 10
MAX_EXPERIENCES = 20
MAX_LANGUAGES = 20
MAX_LANGUAGE_LEN = 50
MAX_HIGHLIGHTS = 10
MAX_HIGHLIGHT_LEN = 300
MIN_YEARS, MAX_YEARS = 0, 60
MIN_END_YEAR, MAX_END_YEAR = 1900, 2100
MAX_DETAIL = 200
MAX_MODEL_VERSION = 100
YEAR_MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
_YEAR_MONTH = re.compile(YEAR_MONTH_PATTERN)

Text = Annotated[str, StringConstraints(max_length=MAX_TEXT)]
Email = Annotated[str, StringConstraints(max_length=MAX_EMAIL)]
Skill = Annotated[str, StringConstraints(max_length=MAX_SKILL_LEN)]
Language = Annotated[str, StringConstraints(max_length=MAX_LANGUAGE_LEN)]
Highlight = Annotated[str, StringConstraints(max_length=MAX_HIGHLIGHT_LEN)]
YearMonth = Annotated[str, StringConstraints(pattern=YEAR_MONTH_PATTERN)]
EndYear = Annotated[int, Field(ge=MIN_END_YEAR, le=MAX_END_YEAR)]
Years = Annotated[float, Field(ge=MIN_YEARS, le=MAX_YEARS)]

# strict: no string-to-number coercion, matching the JSON Schema types.
_STRICT = ConfigDict(extra="forbid", strict=True)


class Education(BaseModel):
    model_config = _STRICT

    school: Text
    degree: Text | None
    major: Text | None
    end_year: EndYear | None


class Experience(BaseModel):
    model_config = _STRICT

    company: Text
    title: Text
    start: YearMonth | None
    end: YearMonth | None
    highlights: Annotated[list[Highlight], Field(max_length=MAX_HIGHLIGHTS)]


class ParsedCV(BaseModel):
    model_config = _STRICT

    full_name: Text | None
    email: Email | None
    skills: Annotated[list[Skill], Field(max_length=MAX_SKILLS)]
    years_of_experience: Years | None
    current_title: Text | None
    education: Annotated[list[Education], Field(max_length=MAX_EDUCATION)]
    experiences: Annotated[list[Experience], Field(max_length=MAX_EXPERIENCES)]
    languages: Annotated[list[Language], Field(max_length=MAX_LANGUAGES)]
    location: Text | None


class EducationDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    school: str
    degree: str | None
    major: str | None
    end_year: int | None


class ExperienceDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    company: str
    title: str
    start: str | None
    end: str | None
    highlights: list[str]


class ParsedCVDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    full_name: str | None
    email: str | None
    skills: list[str]
    years_of_experience: float | None
    current_title: str | None
    education: list[EducationDraft]
    experiences: list[ExperienceDraft]
    languages: list[str]
    location: str | None


# C0 control characters except \t \n \r: Postgres jsonb rejects \u0000 and the
# rest is noise from PDF extraction or the LLM.
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def strip_control(value: str) -> str:
    """Removes C0 control characters other than tab, newline and carriage return."""
    return _CONTROL.sub("", value)


def _text(value: str, limit: int = MAX_TEXT) -> str:
    return strip_control(value)[:limit]


def _clip(value: str | None, limit: int = MAX_TEXT) -> str | None:
    return None if value is None else _text(value, limit)


def _year_month(value: str | None) -> str | None:
    return value if value is not None and _YEAR_MONTH.match(value) else None


def _in_range[N: (int, float)](value: N | None, lo: int, hi: int) -> N | None:
    return value if value is not None and lo <= value <= hi else None


def sanitize(draft: ParsedCVDraft) -> ParsedCV:
    """Clamps an unconstrained LLM draft into a valid ParsedCV (plan decision #3).

    C0 control characters (except \\t \\n \\r) are removed from every string,
    over-long strings are cut, surplus list items dropped, out-of-range numbers
    and malformed dates become None, skills are normalized. Raises
    pydantic.ValidationError only if the result still violates ParsedCV.
    """
    languages = [
        lang[:MAX_LANGUAGE_LEN] for lang in (strip_control(s).strip() for s in draft.languages) if lang
    ]
    return ParsedCV(
        full_name=_clip(draft.full_name),
        email=_clip(draft.email, MAX_EMAIL),
        skills=normalize([strip_control(s) for s in draft.skills]),
        years_of_experience=_in_range(draft.years_of_experience, MIN_YEARS, MAX_YEARS),
        current_title=_clip(draft.current_title),
        education=[
            Education(
                school=_text(draft_edu.school),
                degree=_clip(draft_edu.degree),
                major=_clip(draft_edu.major),
                end_year=_in_range(draft_edu.end_year, MIN_END_YEAR, MAX_END_YEAR),
            )
            for draft_edu in draft.education[:MAX_EDUCATION]
        ],
        experiences=[
            Experience(
                company=_text(draft_exp.company),
                title=_text(draft_exp.title),
                start=_year_month(draft_exp.start),
                end=_year_month(draft_exp.end),
                highlights=[_text(h, MAX_HIGHLIGHT_LEN) for h in draft_exp.highlights[:MAX_HIGHLIGHTS]],
            )
            for draft_exp in draft.experiences[:MAX_EXPERIENCES]
        ],
        languages=languages[:MAX_LANGUAGES],
        location=_clip(draft.location),
    )


FailureReason = Literal[
    "no_text", "not_pdf", "too_large", "llm_refused", "llm_invalid_output", "download_failed"
]


@dataclass(frozen=True)
class ParseSuccess:
    parsed: ParsedCV
    # 1..MAX_MODEL_VERSION characters (cv.parsed.v1.json); enforced so a
    # success can never turn into an event that fails its schema.
    model_version: str
    text_chars: int
    truncated: bool

    def __post_init__(self) -> None:
        if not 1 <= len(self.model_version) <= MAX_MODEL_VERSION:
            raise ValueError(f"model_version must be 1-{MAX_MODEL_VERSION} characters")


@dataclass(frozen=True)
class ParseFailure:
    reason: FailureReason
    # Must not contain PII; cut to MAX_DETAIL characters (cv.parse_failed contract).
    detail: str
    model_version: str | None
    text_chars: int
    truncated: bool

    def __post_init__(self) -> None:
        if len(self.detail) > MAX_DETAIL:
            object.__setattr__(self, "detail", self.detail[:MAX_DETAIL])


ParseOutcome = ParseSuccess | ParseFailure
