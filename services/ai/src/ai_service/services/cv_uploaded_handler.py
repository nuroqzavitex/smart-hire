"""cv.uploaded use case: download the PDF, parse it, store the outcome atomically (spec §5.4)."""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from pydantic import ValidationError

from ai_service.domain.events import SOURCE_CANDIDATE, CvUploadedPayload, Envelope, result_envelope
from ai_service.domain.models import ParseFailure, ParseOutcome
from ai_service.services.cv_parser import MAX_CV_BYTES, parse_cv
from ai_service.services.ports import LlmParser, TextExtractor

log = logging.getLogger(__name__)

CV_UPLOADED = "cv.uploaded"


class DownloadFailed(Exception):
    """The object is missing or could not be downloaded after retries."""


class PermanentError(Exception):
    """Poison input: retrying cannot help, the message goes straight to the DLQ.

    Arguments are fixed strings, never message content.
    """


@dataclass(frozen=True)
class ObjectStat:
    size: int
    content_type: str


class ObjectReader(Protocol):
    def stat(self, key: str) -> ObjectStat: ...  # raises DownloadFailed

    def get(self, key: str) -> bytes: ...  # raises DownloadFailed


class ResultStore(Protocol):
    def is_processed(self, event_id: str) -> bool: ...

    def commit_outcome(
        self, event_id: str, cv: CvUploadedPayload, outcome: ParseOutcome, result: Envelope
    ) -> bool:
        """Atomically records the event, the outcome and the outbox row; False if already processed."""
        ...


def _validate(env: Envelope) -> CvUploadedPayload:
    if env.event_type != CV_UPLOADED or env.event_version != 1 or env.source != SOURCE_CANDIDATE:
        raise PermanentError("unexpected event type, version or source")
    try:
        cv = CvUploadedPayload.model_validate(env.payload)
    except ValidationError:
        # Not chained and not echoed: pydantic errors repeat the offending values.
        raise PermanentError("cv.uploaded payload failed validation") from None
    if cv.object_key != f"cvs/{cv.candidate_id}/{cv.cv_id}.pdf":
        raise PermanentError("cv.uploaded object_key does not match candidate_id/cv_id")
    return cv


class CvUploadedHandler:
    def __init__(
        self,
        storage: ObjectReader,
        extractor: TextExtractor,
        llm: LlmParser,
        store: ResultStore,
        clock: Callable[[], datetime],
        trace_id: Callable[[], str],
    ) -> None:
        self._storage = storage
        self._extractor = extractor
        self._llm = llm
        self._store = store
        self._clock = clock
        self._trace_id = trace_id

    def handle(self, env: Envelope) -> None:
        cv = _validate(env)
        if self._store.is_processed(env.event_id):
            log.info("event already processed", extra={"event_id": env.event_id, "cv_id": str(cv.cv_id)})
            return
        outcome = self._outcome(cv)
        result = result_envelope(
            outcome,
            cv,
            now=self._clock(),
            trace_id=self._trace_id(),
            correlation_id=env.correlation_id,
        )
        applied = self._store.commit_outcome(env.event_id, cv, outcome, result)
        fields = {"event_id": env.event_id, "cv_id": str(cv.cv_id), "attempt_id": cv.attempt_id}
        if isinstance(outcome, ParseFailure):
            fields["reason"] = outcome.reason
        log.info("cv processed" if applied else "event already processed", extra=fields)

    def _outcome(self, cv: CvUploadedPayload) -> ParseOutcome:
        try:
            if self._storage.stat(cv.object_key).size > MAX_CV_BYTES:
                return ParseFailure("too_large", f"file exceeds {MAX_CV_BYTES} bytes", None, 0, False)
            data = self._storage.get(cv.object_key)
        except DownloadFailed:
            return ParseFailure("download_failed", "object could not be downloaded", None, 0, False)
        return parse_cv(data, self._extractor, self._llm)
