import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from minio.error import S3Error
from pdfs import pdf

from ai_service.adapters.llm import FakeLlmParser
from ai_service.adapters.pdf import PyMuPdfExtractor
from ai_service.adapters.storage import MinioObjectReader, StorageFault
from ai_service.domain.events import CvUploadedPayload, Envelope, new_envelope
from ai_service.domain.models import ParsedCV, ParseFailure, ParseOutcome, ParseSuccess
from ai_service.services.cv_parser import MAX_CV_BYTES
from ai_service.services.cv_uploaded_handler import (
    CvUploadedHandler,
    DownloadFailed,
    ObjectStat,
    PermanentError,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
CV_LINES = ["Nguyen Van Nam", "Skills: golang, k8s, postgres", "x" * 60]


class FakeStorage:
    def __init__(self, data: bytes = b"", size: int | None = None, fail: bool = False) -> None:
        self.data = data
        self.size = len(data) if size is None else size
        self.fail = fail
        self.gets = 0

    def stat(self, key: str) -> ObjectStat:
        if self.fail:
            raise DownloadFailed
        return ObjectStat(self.size, "application/pdf")

    def get(self, key: str) -> bytes:
        self.gets += 1
        if self.fail:
            raise DownloadFailed
        return self.data


class FakeStore:
    def __init__(self, processed: bool = False) -> None:
        self.processed = processed
        self.commits: list[tuple[str, CvUploadedPayload, ParseOutcome, Envelope]] = []

    def is_processed(self, event_id: str) -> bool:
        return self.processed

    def commit_outcome(
        self, event_id: str, cv: CvUploadedPayload, outcome: ParseOutcome, result: Envelope
    ) -> bool:
        self.commits.append((event_id, cv, outcome, result))
        return True


class CountingLlm(FakeLlmParser):
    calls = 0

    def parse(self, cv_text: str) -> ParsedCV:
        CountingLlm.calls += 1
        return super().parse(cv_text)


def _payload(**override: Any) -> dict[str, Any]:
    cand, cv = uuid4(), uuid4()
    p = {
        "cv_id": str(cv),
        "candidate_id": str(cand),
        "object_key": f"cvs/{cand}/{cv}.pdf",
        "attempt_id": "01JABCDEFGHJKMNPQRSTVWXYZ0",
    }
    p.update(override)
    return p


def _env(payload: dict[str, Any], **kw: Any) -> Envelope:
    env = new_envelope("cv.uploaded", payload, now=NOW, trace_id="t", correlation_id="corr")
    return env.model_copy(update={"source": "candidate-service", **kw})


def _handler(storage: FakeStorage, store: FakeStore) -> CvUploadedHandler:
    return CvUploadedHandler(
        storage, PyMuPdfExtractor(), CountingLlm(), store, clock=lambda: NOW, trace_id=lambda: "trace-1"
    )


def test_success_commits_parsed():
    store = FakeStore()
    _handler(FakeStorage(pdf(CV_LINES)), store).handle(_env(_payload()))
    event_id, cv, outcome, result = store.commits[0]
    assert isinstance(outcome, ParseSuccess)
    assert result.event_type == "cv.parsed"
    assert result.correlation_id == "corr"
    assert result.trace_id == "trace-1"
    assert result.payload["attempt_id"] == cv.attempt_id
    assert event_id


def test_handler_permanent_on_bad_payload():
    p = _payload()
    del p["attempt_id"]
    with pytest.raises(PermanentError):
        _handler(FakeStorage(), FakeStore()).handle(_env(p))


@pytest.mark.parametrize(
    "env_kw",
    [{"event_type": "cv.parsed"}, {"event_version": 2}, {"source": "other"}],
)
def test_handler_permanent_on_bad_envelope(env_kw: dict[str, Any]):
    with pytest.raises(PermanentError):
        _handler(FakeStorage(), FakeStore()).handle(_env(_payload(), **env_kw))


def test_handler_permanent_on_object_key_mismatch():
    other = f"cvs/{uuid4()}/{uuid4()}.pdf"
    with pytest.raises(PermanentError) as exc:
        _handler(FakeStorage(), FakeStore()).handle(_env(_payload(object_key=other)))
    assert other not in str(exc.value)


def test_handler_too_large_skips_download():
    storage, store = FakeStorage(b"", size=MAX_CV_BYTES + 1), FakeStore()
    _handler(storage, store).handle(_env(_payload()))
    outcome = store.commits[0][2]
    assert isinstance(outcome, ParseFailure) and outcome.reason == "too_large"
    assert storage.gets == 0


def test_handler_object_grown_after_stat_is_too_large():
    # The presigned PUT URL outlives complete: the object can be replaced
    # between stat and get, so the downloaded size is checked again.
    storage = FakeStorage(b"%PDF-" + b"x" * MAX_CV_BYTES, size=1024)
    store = FakeStore()
    CountingLlm.calls = 0
    _handler(storage, store).handle(_env(_payload()))
    outcome = store.commits[0][2]
    assert isinstance(outcome, ParseFailure) and outcome.reason == "too_large"
    assert storage.gets == 1 and CountingLlm.calls == 0


def test_handler_download_failed_emits_failure():
    store = FakeStore()
    _handler(FakeStorage(fail=True), store).handle(_env(_payload()))
    outcome, result = store.commits[0][2], store.commits[0][3]
    assert isinstance(outcome, ParseFailure) and outcome.reason == "download_failed"
    assert result.event_type == "cv.parse_failed"


def test_handler_skips_processed():
    storage, store = FakeStorage(pdf(CV_LINES)), FakeStore(processed=True)
    CountingLlm.calls = 0
    _handler(storage, store).handle(_env(_payload()))
    assert CountingLlm.calls == 0 and storage.gets == 0 and store.commits == []


def test_handler_logs_no_object_key_or_cv_text(caplog: pytest.LogCaptureFixture):
    p = _payload()
    with caplog.at_level(logging.INFO):
        _handler(FakeStorage(pdf(CV_LINES)), FakeStore()).handle(_env(p))
    assert p["object_key"] not in caplog.text
    assert "Nguyen" not in caplog.text


# --- MinioObjectReader ---


def _s3(code: str, status: int) -> S3Error:
    response = SimpleNamespace(status=status)
    return S3Error(response, code, "msg", "/cvs/secret-key", "rid", "hid")  # type: ignore[arg-type]


class FakeResponse:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self) -> bytes:
        return self.data

    def close(self) -> None:
        pass

    def release_conn(self) -> None:
        pass


class FakeMinio:
    def __init__(self, errors: list[Exception], data: bytes = b"pdf") -> None:
        self.errors = errors
        self.calls = 0
        self.data = data
        self.lengths: list[int] = []

    def _maybe_fail(self) -> None:
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)

    def stat_object(self, bucket: str, key: str) -> Any:
        self._maybe_fail()
        return SimpleNamespace(size=len(self.data), content_type="application/pdf")

    def get_object(self, bucket: str, key: str, offset: int = 0, length: int = 0) -> FakeResponse:
        self._maybe_fail()
        self.lengths.append(length)
        # Like S3 with a Range header: at most `length` bytes come back.
        return FakeResponse(self.data[offset : offset + length] if length else self.data)


def _reader(client: FakeMinio, sleeps: list[float]) -> MinioObjectReader:
    return MinioObjectReader(client, "cvs", sleep=sleeps.append)  # type: ignore[arg-type]


def test_minio_reader_retries_transient():
    sleeps: list[float] = []
    client = FakeMinio([ConnectionError("net https://x/cvs/secret-key"), OSError("net")])
    assert _reader(client, sleeps).get("cvs/a/b.pdf") == b"pdf"
    assert client.calls == 3
    assert sleeps == [0.2, 0.4]


def test_minio_reader_stat_ok():
    assert _reader(FakeMinio([]), []).stat("k") == ObjectStat(3, "application/pdf")


def test_minio_reader_no_such_key_not_retried():
    sleeps: list[float] = []
    client = FakeMinio([_s3("NoSuchKey", 404)])
    with pytest.raises(DownloadFailed):
        _reader(client, sleeps).get("cvs/a/b.pdf")
    assert client.calls == 1 and sleeps == []


@pytest.mark.parametrize("code", ["AccessDenied", "NoSuchBucket"])
def test_minio_reader_deployment_fault_not_retried(code: str):
    sleeps: list[float] = []
    client = FakeMinio([_s3(code, 403)])
    with pytest.raises(StorageFault) as exc:
        _reader(client, sleeps).get("cvs/a/b.pdf")
    assert not isinstance(exc.value, DownloadFailed)
    assert client.calls == 1 and sleeps == []
    assert "secret-key" not in str(exc.value) and "cvs/" not in str(exc.value)


def test_minio_reader_5xx_retries_then_download_failed():
    sleeps: list[float] = []
    client = FakeMinio([_s3("InternalError", 500)] * 5)
    with pytest.raises(DownloadFailed) as exc:
        _reader(client, sleeps).stat("cvs/a/b.pdf")
    assert client.calls == 3 and sleeps == [0.2, 0.4]
    assert "secret-key" not in str(exc.value)


def test_minio_reader_get_reads_at_most_limit_plus_one():
    client = FakeMinio([], data=b"%PDF-" + b"x" * (MAX_CV_BYTES + 100))
    data = _reader(client, []).get("cvs/a/b.pdf")
    assert client.lengths == [MAX_CV_BYTES + 1]
    assert len(data) == MAX_CV_BYTES + 1


def test_minio_reader_get_empty_object_invalid_range():
    # A ranged GET of a 0-byte object fails with InvalidRange (416): the object is empty.
    sleeps: list[float] = []
    client = FakeMinio([_s3("InvalidRange", 416)])
    assert _reader(client, sleeps).get("cvs/a/b.pdf") == b""
    assert client.calls == 1 and sleeps == []
