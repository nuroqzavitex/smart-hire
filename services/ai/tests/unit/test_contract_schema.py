"""Keeps the pydantic models and contracts/events JSON Schemas in lock-step."""

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel, ValidationError

from ai_service.domain.events import CvUploadedPayload, dump_envelope, result_envelope
from ai_service.domain.models import Education, Experience, ParsedCV, ParseFailure, ParseSuccess

NOW = datetime(2026, 11, 11, 8, 30, 15, tzinfo=UTC)
CV_ID = uuid.UUID("0b8f3c9e-6f1a-4c55-9a51-3c1f2e9b7d10")
CANDIDATE_ID = uuid.UUID("7d0c2a4e-1b9f-4e2a-8c3d-5f6a7b8c9d0e")


def _schema(contracts_dir: Path, event_type: str) -> dict[str, Any]:
    return json.loads((contracts_dir / f"{event_type}.v1.json").read_text(encoding="utf-8"))


def _validator(schema: dict[str, Any]) -> Draft202012Validator:
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _parsed_cv_validator(contracts_dir: Path) -> Draft202012Validator:
    schema = _schema(contracts_dir, "cv.parsed")
    sub = {"$defs": schema["$defs"], "$ref": "#/$defs/ParsedCV"}
    return _validator(sub)


def _cv() -> CvUploadedPayload:
    return CvUploadedPayload(
        cv_id=CV_ID,
        candidate_id=CANDIDATE_ID,
        object_key=f"cvs/{CANDIDATE_ID}/{CV_ID}.pdf",
        attempt_id="01JCX3T2ZQ8K4N5P6R7S8T9V0W",
    )


def _fixture_files(contracts_dir: Path, kind: str) -> list[Path]:
    files = sorted((contracts_dir / "testdata" / "parsed_cv" / kind).glob("*.json"))
    assert files, f"no {kind} fixtures"
    return files


def test_format_checker_checks_date_time() -> None:
    # Without rfc3339-validator installed, jsonschema silently skips date-time.
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate("not a date", {"format": "date-time"}, format_checker=FormatChecker())


def test_result_envelopes_match_contract(contracts_dir: Path) -> None:
    full = json.loads((contracts_dir / "testdata/parsed_cv/valid/full.json").read_text(encoding="utf-8"))
    success = ParseSuccess(
        parsed=ParsedCV.model_validate(full),
        model_version="anthropic:claude-opus-5-5",
        text_chars=9,
        truncated=True,
    )
    env = result_envelope(
        success, _cv(), now=NOW, trace_id="4bf92f3577b34da6a3ce929d0e0e4736", correlation_id=""
    )
    _validator(_schema(contracts_dir, "cv.parsed")).validate(json.loads(dump_envelope(env)))

    for reason in ("no_text", "not_pdf", "too_large", "llm_refused", "llm_invalid_output", "download_failed"):
        failure = ParseFailure(
            reason=reason, detail="d" * 250, model_version=None, text_chars=0, truncated=False
        )
        env = result_envelope(failure, _cv(), now=NOW, trace_id="", correlation_id="req-1")
        _validator(_schema(contracts_dir, "cv.parse_failed")).validate(json.loads(dump_envelope(env)))


def test_cv_uploaded_payload_matches_contract(contracts_dir: Path) -> None:
    schema = _schema(contracts_dir, "cv.uploaded")
    payload_schema = schema["properties"]["payload"]
    assert set(CvUploadedPayload.model_fields) == set(payload_schema["properties"])
    _validator(payload_schema).validate(_cv().model_dump(mode="json"))


def test_parsed_cv_limit_fixtures_present(contracts_dir: Path) -> None:
    # Plan decision #2 limits, shared with the Go eventschema tests.
    names = {kind: {p.stem for p in _fixture_files(contracts_dir, kind)} for kind in ("valid", "invalid")}
    assert "boundaries" in names["valid"]
    assert {"education_11", "languages_21", "full_name_201", "email_255"} <= names["invalid"]


@pytest.mark.parametrize("kind", ["valid", "invalid"])
def test_parsed_cv_fixtures_agree_with_contract(contracts_dir: Path, kind: str) -> None:
    validator = _parsed_cv_validator(contracts_dir)
    for path in _fixture_files(contracts_dir, kind):
        raw = path.read_text(encoding="utf-8")
        if kind == "valid":
            ParsedCV.model_validate_json(raw)
            validator.validate(json.loads(raw))
        else:
            with pytest.raises(ValidationError):
                ParsedCV.model_validate_json(raw)
            assert not validator.is_valid(json.loads(raw)), path.name


@pytest.mark.parametrize(
    ("model", "def_name"),
    [(ParsedCV, "ParsedCV"), (Education, "Education"), (Experience, "Experience")],
)
def test_parsed_cv_fields_match_contract(contracts_dir: Path, model: type[BaseModel], def_name: str) -> None:
    schema_def = _schema(contracts_dir, "cv.parsed")["$defs"][def_name]
    assert set(model.model_fields) == set(schema_def["properties"])
    required = {name for name, f in model.model_fields.items() if f.is_required()}
    assert required == set(schema_def["required"])
    assert schema_def["additionalProperties"] is False
    assert model.model_config.get("extra") == "forbid"
