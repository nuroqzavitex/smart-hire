from typing import Any

import pytest
from pydantic import ValidationError

from ai_service.domain.models import (
    EducationDraft,
    ExperienceDraft,
    ParsedCV,
    ParsedCVDraft,
    ParseFailure,
    ParseSuccess,
    sanitize,
)


def _valid() -> dict[str, Any]:
    return {
        "full_name": "Nguyen Van A",
        "email": "a@example.com",
        "skills": ["Go"],
        "years_of_experience": 3.5,
        "current_title": "Engineer",
        "education": [{"school": "HUST", "degree": None, "major": None, "end_year": 2018}],
        "experiences": [
            {"company": "C", "title": "T", "start": "2021-03", "end": None, "highlights": ["did things"]}
        ],
        "languages": ["English"],
        "location": "Ha Noi",
    }


def test_parsed_cv_accepts_valid() -> None:
    p = ParsedCV.model_validate(_valid())
    assert p.skills == ["Go"]
    assert p.experiences[0].start == "2021-03"


def _mutations() -> list[tuple[str, Any]]:
    def set_(path: list[str | int], value: Any) -> Any:
        def apply(d: dict[str, Any]) -> None:
            cur: Any = d
            for key in path[:-1]:
                cur = cur[key]
            cur[path[-1]] = value

        return apply

    def delete(key: str) -> Any:
        return lambda d: d.pop(key)

    return [
        ("51 skills", set_(["skills"], [f"s{i}" for i in range(51)])),
        ("skill 51 chars", set_(["skills"], ["x" * 51])),
        ("11 highlights", set_(["experiences", 0, "highlights"], ["h"] * 11)),
        ("highlight 301 chars", set_(["experiences", 0, "highlights"], ["h" * 301])),
        ("years 61", set_(["years_of_experience"], 61)),
        ("years -1", set_(["years_of_experience"], -1)),
        ("start 2024-13", set_(["experiences", 0, "start"], "2024-13")),
        ("end 2024-1", set_(["experiences", 0, "end"], "2024-1")),
        ("extra field", set_(["phone"], "123")),
        ("extra field nested", set_(["experiences", 0, "salary"], "1")),
        ("missing skills", delete("skills")),
        ("missing nullable full_name", delete("full_name")),
        ("full_name 201", set_(["full_name"], "a" * 201)),
        ("email 255", set_(["email"], "a" * 255)),
        (
            "11 education",
            set_(["education"], [{"school": "S", "degree": None, "major": None, "end_year": None}] * 11),
        ),
        ("21 experiences", set_(["experiences"], [_valid()["experiences"][0]] * 21)),
        ("21 languages", set_(["languages"], ["en"] * 21)),
        ("language 51 chars", set_(["languages"], ["x" * 51])),
        ("end_year 1899", set_(["education", 0, "end_year"], 1899)),
        ("end_year 2101", set_(["education", 0, "end_year"], 2101)),
        ("school 201", set_(["education", 0, "school"], "s" * 201)),
        ("company 201", set_(["experiences", 0, "company"], "c" * 201)),
    ]


@pytest.mark.parametrize(("name", "mutate"), _mutations(), ids=[m[0] for m in _mutations()])
def test_parsed_cv_limits(name: str, mutate: Any) -> None:
    data = _valid()
    mutate(data)
    with pytest.raises(ValidationError):
        ParsedCV.model_validate(data)


def test_parsed_cv_limits_boundaries_accepted() -> None:
    data = _valid()
    data["skills"] = [f"s{i}" for i in range(49)] + ["x" * 50]
    data["years_of_experience"] = 60
    data["experiences"][0]["highlights"] = ["h" * 300] * 10
    data["education"][0]["end_year"] = 2100
    data["experiences"][0]["end"] = "2024-12"
    ParsedCV.model_validate(data)


def test_sanitize_clamps() -> None:
    d = ParsedCVDraft(
        full_name="A" * 300,
        email=None,
        skills=["golang"] * 3 + [f"s{i}" for i in range(60)],
        years_of_experience=99,
        current_title=None,
        education=[],
        experiences=[
            ExperienceDraft(
                company="C", title="T", start="2024-1", end="2024-05", highlights=["h" * 400] * 12
            )
        ],
        languages=[],
        location=None,
    )
    p = sanitize(d)
    assert isinstance(p, ParsedCV)
    assert len(p.full_name or "") == 200
    assert p.years_of_experience is None
    assert p.skills[0] == "Go"
    assert len(p.skills) == 50
    assert p.experiences[0].start is None
    assert p.experiences[0].end == "2024-05"
    assert len(p.experiences[0].highlights) == 10
    assert all(len(h) == 300 for h in p.experiences[0].highlights)


def test_sanitize_clamps_lists_and_nested_fields() -> None:
    d = ParsedCVDraft(
        full_name=None,
        email="e" * 300,
        skills=[],
        years_of_experience=-0.5,
        current_title="t" * 201,
        education=[EducationDraft(school="s" * 250, degree="d" * 250, major=None, end_year=1800)] * 12,
        experiences=[ExperienceDraft(company="c", title="t", start=None, end="May 2020", highlights=[])] * 25,
        languages=["Vietnamese", "  ", "l" * 60] + ["English"] * 30,
        location="l" * 201,
    )
    p = sanitize(d)
    assert len(p.email or "") == 254
    assert p.years_of_experience is None
    assert len(p.current_title or "") == 200
    assert len(p.location or "") == 200
    assert len(p.education) == 10
    assert len(p.education[0].school) == 200
    assert len(p.education[0].degree or "") == 200
    assert p.education[0].end_year is None
    assert len(p.experiences) == 20
    assert p.experiences[0].end is None
    assert len(p.languages) == 20
    assert all(0 < len(lang) <= 50 for lang in p.languages)
    assert p.languages[0] == "Vietnamese"


def test_sanitize_keeps_valid_values() -> None:
    d = ParsedCVDraft.model_validate(_valid())
    assert sanitize(d) == ParsedCV.model_validate(_valid())


def test_parse_failure_truncates_detail() -> None:
    f = ParseFailure(reason="no_text", detail="x" * 500, model_version=None, text_chars=3, truncated=False)
    assert len(f.detail) == 200
    short = ParseFailure(
        reason="not_pdf", detail="bad magic", model_version=None, text_chars=0, truncated=False
    )
    assert short.detail == "bad magic"


def test_parse_success_is_frozen() -> None:
    s = ParseSuccess(
        parsed=ParsedCV.model_validate(_valid()),
        model_version="fake:rules-v1",
        text_chars=10,
        truncated=False,
    )
    with pytest.raises(AttributeError):
        s.text_chars = 11  # type: ignore[misc]


@pytest.mark.parametrize("model_version", ["", "m" * 101])
def test_parse_success_bounds_model_version(model_version: str) -> None:
    # cv.parsed.v1.json: model_version minLength 1, maxLength 100 (E-R11).
    with pytest.raises(ValueError, match="model_version"):
        ParseSuccess(
            parsed=ParsedCV.model_validate(_valid()),
            model_version=model_version,
            text_chars=10,
            truncated=False,
        )


def test_parse_success_accepts_100_char_model_version() -> None:
    s = ParseSuccess(
        parsed=ParsedCV.model_validate(_valid()), model_version="m" * 100, text_chars=10, truncated=False
    )
    assert len(s.model_version) == 100
