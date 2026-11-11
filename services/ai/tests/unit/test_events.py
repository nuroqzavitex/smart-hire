import json
import re
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from ai_service.domain.events import (
    SOURCE,
    CvUploadedPayload,
    Envelope,
    dump_envelope,
    new_envelope,
    result_envelope,
)
from ai_service.domain.models import ParsedCV, ParseFailure, ParseSuccess

ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
NOW = datetime(2026, 11, 11, 8, 30, 15, 123456, tzinfo=UTC)
CV_ID = uuid.UUID("0b8f3c9e-6f1a-4c55-9a51-3c1f2e9b7d10")
CANDIDATE_ID = uuid.UUID("7d0c2a4e-1b9f-4e2a-8c3d-5f6a7b8c9d0e")
ATTEMPT_ID = "01JCX3T2ZQ8K4N5P6R7S8T9V0W"


def _cv() -> CvUploadedPayload:
    return CvUploadedPayload(
        cv_id=CV_ID,
        candidate_id=CANDIDATE_ID,
        object_key=f"cvs/{CANDIDATE_ID}/{CV_ID}.pdf",
        attempt_id=ATTEMPT_ID,
    )


def _parsed() -> ParsedCV:
    return ParsedCV(
        full_name="A",
        email=None,
        skills=["Go"],
        years_of_experience=None,
        current_title=None,
        education=[],
        experiences=[],
        languages=[],
        location=None,
    )


def test_cv_uploaded_payload_validation() -> None:
    p = CvUploadedPayload.model_validate(
        {
            "cv_id": str(CV_ID),
            "candidate_id": str(CANDIDATE_ID),
            "object_key": f"cvs/{CANDIDATE_ID}/{CV_ID}.pdf",
            "attempt_id": ATTEMPT_ID,
        }
    )
    assert p.cv_id == CV_ID
    with pytest.raises(ValidationError):
        CvUploadedPayload.model_validate({**p.model_dump(mode="json"), "attempt_id": "not-a-ulid"})
    with pytest.raises(ValidationError):
        CvUploadedPayload.model_validate({**p.model_dump(mode="json"), "extra": 1})
    with pytest.raises(ValidationError):
        CvUploadedPayload.model_validate({**p.model_dump(mode="json"), "cv_id": "nope"})


def test_new_envelope() -> None:
    env = new_envelope("cv.parsed", {"a": 1}, now=NOW, trace_id="t" * 32, correlation_id="corr")
    assert isinstance(env, Envelope)
    assert ULID_RE.match(env.event_id)
    assert env.event_type == "cv.parsed"
    assert env.event_version == 1
    assert env.source == SOURCE == "ai-service"
    assert env.occurred_at == NOW
    assert env.trace_id == "t" * 32
    assert env.correlation_id == "corr"
    assert env.payload == {"a": 1}
    other = new_envelope("cv.parsed", {}, now=NOW, trace_id="", correlation_id="")
    assert other.event_id != env.event_id


def test_dump_envelope_occurred_at_is_utc_z() -> None:
    plus7 = timezone(timedelta(hours=7))
    env = new_envelope("cv.parsed", {}, now=NOW.astimezone(plus7), trace_id="", correlation_id="")
    doc = json.loads(dump_envelope(env))
    assert doc["occurred_at"] == "2026-11-11T08:30:15.123456Z"
    assert set(doc) == {
        "event_id",
        "event_type",
        "event_version",
        "source",
        "occurred_at",
        "trace_id",
        "correlation_id",
        "payload",
    }


def test_result_envelope_success_payload() -> None:
    outcome = ParseSuccess(parsed=_parsed(), model_version="fake:rules-v1", text_chars=120, truncated=False)
    env = result_envelope(outcome, _cv(), now=NOW, trace_id="tr", correlation_id="co")
    assert env.event_type == "cv.parsed"
    assert env.payload == {
        "cv_id": str(CV_ID),
        "candidate_id": str(CANDIDATE_ID),
        "attempt_id": ATTEMPT_ID,
        "parsed": _parsed().model_dump(mode="json"),
        "model_version": "fake:rules-v1",
    }


def test_result_envelope_failure_payload() -> None:
    outcome = ParseFailure(
        reason="no_text", detail="text too short", model_version=None, text_chars=3, truncated=False
    )
    env = result_envelope(outcome, _cv(), now=NOW, trace_id="tr", correlation_id="co")
    assert env.event_type == "cv.parse_failed"
    assert env.payload == {
        "cv_id": str(CV_ID),
        "candidate_id": str(CANDIDATE_ID),
        "attempt_id": ATTEMPT_ID,
        "reason": "no_text",
        "detail": "text too short",
    }
