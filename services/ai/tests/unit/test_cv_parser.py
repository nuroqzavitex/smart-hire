from typing import ClassVar

import pytest
from pdfs import encrypted_pdf, image_only_pdf, pdf

from ai_service.adapters.llm import FakeLlmParser
from ai_service.adapters.pdf import PyMuPdfExtractor
from ai_service.domain.models import ParsedCV, ParseFailure, ParseOutcome, ParseSuccess
from ai_service.services.cv_parser import MAX_CV_BYTES, MAX_LLM_CHARS, MIN_TEXT_CHARS, parse_cv
from ai_service.services.ports import LlmInvalidOutput, LlmRefused

CV_LINES = ["Nguyen Van Nam", "nguyen.nam@example.com", "Skills: golang, k8s, postgres", "x" * 60]
ext = PyMuPdfExtractor()


class RaisingLlm:
    model_version = "raising:v1"

    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def parse(self, cv_text: str) -> ParsedCV:
        raise self.exc


class RecordingLlm(FakeLlmParser):
    last_text: ClassVar[str] = ""

    def parse(self, cv_text: str) -> ParsedCV:
        RecordingLlm.last_text = cv_text
        return super().parse(cv_text)


@pytest.fixture
def fake_llm() -> FakeLlmParser:
    return FakeLlmParser()


def _failures(fake_llm: FakeLlmParser) -> dict[str, ParseOutcome]:
    cv = pdf(CV_LINES)
    return {
        "too_large": parse_cv(b"%PDF-" + b"0" * MAX_CV_BYTES, ext, fake_llm),
        "html": parse_cv(b"<html>", ext, fake_llm),
        "empty": parse_cv(b"", ext, fake_llm),
        "corrupt": parse_cv(b"%PDF-1.4 garbage", ext, fake_llm),
        "encrypted": parse_cv(encrypted_pdf("secret"), ext, fake_llm),
        "image_only": parse_cv(image_only_pdf(), ext, fake_llm),
        "short_text": parse_cv(pdf(["x" * 49]), ext, fake_llm),
        "refused": parse_cv(cv, ext, RaisingLlm(LlmRefused("cyber"))),
        "invalid": parse_cv(cv, ext, RaisingLlm(LlmInvalidOutput("validation"))),
    }


def test_parse_cv_reasons(fake_llm: FakeLlmParser) -> None:
    out = _failures(fake_llm)
    reasons = {name: getattr(o, "reason", None) for name, o in out.items()}
    assert reasons == {
        "too_large": "too_large",
        "html": "not_pdf",
        "empty": "not_pdf",
        "corrupt": "not_pdf",
        "encrypted": "not_pdf",
        "image_only": "no_text",
        "short_text": "no_text",
        "refused": "llm_refused",
        "invalid": "llm_invalid_output",
    }
    assert out["short_text"].text_chars == 49
    refused = out["refused"]
    assert isinstance(refused, ParseFailure)
    assert refused.model_version == "raising:v1"
    assert refused.text_chars > 0
    too_large = out["too_large"]
    assert isinstance(too_large, ParseFailure)
    assert too_large.model_version is None
    assert too_large.text_chars == 0


def test_parse_cv_exactly_max_bytes_is_not_too_large(fake_llm: FakeLlmParser) -> None:
    data = b"%PDF-" + b"0" * (MAX_CV_BYTES - 5)
    assert len(data) == MAX_CV_BYTES
    out = parse_cv(data, ext, fake_llm)
    assert isinstance(out, ParseFailure)
    assert out.reason == "not_pdf"


def test_parse_cv_failure_detail_has_no_cv_text(fake_llm: FakeLlmParser) -> None:
    for name, out in _failures(fake_llm).items():
        assert isinstance(out, ParseFailure), name
        assert out.detail, name
        assert len(out.detail) <= 200, name
        lowered = out.detail.lower()
        assert "nguyen" not in lowered, name
        assert "nguyen.nam@example.com" not in lowered, name
        assert "golang" not in lowered, name
    invalid = parse_cv(pdf(CV_LINES), ext, RaisingLlm(LlmInvalidOutput("validation")))
    assert isinstance(invalid, ParseFailure)
    assert invalid.detail == "llm output failed validation: validation"


def test_parse_cv_truncates_and_normalizes() -> None:
    out = parse_cv(pdf(["Tran Thi B", "b@x.io", "golang k8s"] + ["y" * 100] * 700), ext, RecordingLlm())
    assert isinstance(out, ParseSuccess)
    assert out.truncated
    assert len(RecordingLlm.last_text) == MAX_LLM_CHARS == 60_000
    assert out.parsed.skills == ["Go", "Kubernetes"]
    assert out.parsed.full_name == "Tran Thi B"
    assert out.text_chars > MAX_LLM_CHARS
    assert out.model_version == "fake:rules-v1"


def test_parse_cv_success_not_truncated(fake_llm: FakeLlmParser) -> None:
    out = parse_cv(pdf(CV_LINES), ext, fake_llm)
    assert isinstance(out, ParseSuccess)
    assert not out.truncated
    assert out.text_chars >= MIN_TEXT_CHARS
    assert out.parsed.email == "nguyen.nam@example.com"
    assert out.parsed.skills == ["Go", "Kubernetes", "PostgreSQL"]


def test_parse_cv_normalizes_llm_skills() -> None:
    class AliasLlm(FakeLlmParser):
        def parse(self, cv_text: str) -> ParsedCV:
            return super().parse(cv_text).model_copy(update={"skills": ["golang", "K8S", "Go"]})

    out = parse_cv(pdf(CV_LINES), ext, AliasLlm())
    assert isinstance(out, ParseSuccess)
    assert out.parsed.skills == ["Go", "Kubernetes"]


def test_parse_cv_unexpected_error_propagates() -> None:
    with pytest.raises(ConnectionError):
        parse_cv(pdf(CV_LINES), ext, RaisingLlm(ConnectionError()))
