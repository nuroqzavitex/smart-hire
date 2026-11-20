"""Event envelope (spec §7.1) and the cv.* payloads the AI service reads/writes."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints
from ulid import ULID

from ai_service.domain.models import ParsedCV, ParseOutcome, ParseSuccess

SOURCE = "ai-service"
SOURCE_CANDIDATE = "candidate-service"
SOURCE_JOB = "job-service"
EVENT_VERSION = 1
CV_PARSED = "cv.parsed"
CV_PARSE_FAILED = "cv.parse_failed"

ULID_PATTERN = r"^[0-9A-HJKMNP-TV-Z]{26}$"
UlidStr = Annotated[str, StringConstraints(pattern=ULID_PATTERN)]


class Envelope(BaseModel):
    event_id: str
    event_type: str
    event_version: int
    source: str
    occurred_at: AwareDatetime
    trace_id: str
    correlation_id: str
    payload: dict[str, Any]


class CvUploadedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cv_id: UUID
    candidate_id: UUID
    object_key: str
    attempt_id: UlidStr


class JobPayload(BaseModel):
    """job.created / job.updated payload (contracts/events/job.*.v1.json)."""

    model_config = ConfigDict(extra="ignore")

    job_id: UUID
    recruiter_id: UUID
    title: str
    description: str
    location: str | None
    required_skills: list[str]
    nice_to_have_skills: list[str]
    level: str
    salary_min: int | None
    salary_max: int | None
    status: str
    version: Annotated[int, Field(ge=1)]


class JobRefPayload(BaseModel):
    """job.closed / job.deleted payload."""

    model_config = ConfigDict(extra="ignore")

    job_id: UUID
    version: Annotated[int, Field(ge=1)]


class CvUpdatedPayload(BaseModel):
    """cv.updated payload (contracts/events/cv.updated.v1.json)."""

    model_config = ConfigDict(extra="forbid")

    cv_id: UUID
    candidate_id: UUID
    version: Annotated[int, Field(ge=1)]
    status: Literal["UPLOADED", "PARSED", "FAILED"]
    parsed: ParsedCV | None
    parsed_at: AwareDatetime | None


def new_envelope(
    event_type: str,
    payload: Mapping[str, Any],
    *,
    now: datetime,
    trace_id: str,
    correlation_id: str,
) -> Envelope:
    """Builds an envelope with a fresh ULID event_id and event_version 1."""
    return Envelope(
        event_id=str(ULID()),
        event_type=event_type,
        event_version=EVENT_VERSION,
        source=SOURCE,
        occurred_at=now,
        trace_id=trace_id,
        correlation_id=correlation_id,
        payload=dict(payload),
    )


def result_envelope(
    outcome: ParseOutcome,
    cv: CvUploadedPayload,
    *,
    now: datetime,
    trace_id: str,
    correlation_id: str,
) -> Envelope:
    """Maps a parse outcome to a cv.parsed or cv.parse_failed envelope (spec §4)."""
    payload: dict[str, Any] = {
        "cv_id": str(cv.cv_id),
        "candidate_id": str(cv.candidate_id),
        "attempt_id": cv.attempt_id,
    }
    if isinstance(outcome, ParseSuccess):
        event_type = CV_PARSED
        payload["parsed"] = outcome.parsed.model_dump(mode="json")
        payload["model_version"] = outcome.model_version
    else:
        event_type = CV_PARSE_FAILED
        payload["reason"] = outcome.reason
        payload["detail"] = outcome.detail
    return new_envelope(event_type, payload, now=now, trace_id=trace_id, correlation_id=correlation_id)


def _rfc3339_utc(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat().replace("+00:00", "Z")


def dump_envelope(env: Envelope) -> bytes:
    """Serializes the envelope as JSON with occurred_at in RFC 3339 UTC ("Z")."""
    doc = env.model_dump(mode="json")
    doc["occurred_at"] = _rfc3339_utc(env.occurred_at)
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
