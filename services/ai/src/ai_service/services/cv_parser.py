"""CV parse use case: PDF bytes -> ParseOutcome (spec §5.3)."""

from ai_service.domain.models import FailureReason, ParseFailure, ParseOutcome, ParseSuccess, strip_control
from ai_service.domain.skills import normalize
from ai_service.services.ports import LlmInvalidOutput, LlmParser, LlmRefused, PdfUnreadable, TextExtractor

MAX_CV_BYTES = 5 * 1024 * 1024
MIN_TEXT_CHARS = 50
MAX_LLM_CHARS = 60_000
PDF_MAGIC = b"%PDF-"


def _fail(
    reason: FailureReason,
    detail: str,
    *,
    model_version: str | None = None,
    text_chars: int = 0,
    truncated: bool = False,
) -> ParseFailure:
    return ParseFailure(
        reason=reason, detail=detail, model_version=model_version, text_chars=text_chars, truncated=truncated
    )


def parse_cv(data: bytes, extractor: TextExtractor, llm: LlmParser) -> ParseOutcome:
    """Validates and extracts the PDF, then asks the LLM for a ParsedCV.

    Expected failures become a ParseFailure whose detail is a fixed, CV-free
    message; anything else (LLM connection errors, ...) propagates so the
    consumer can retry.
    """
    if len(data) > MAX_CV_BYTES:
        return _fail("too_large", f"file exceeds {MAX_CV_BYTES} bytes")
    if not data.startswith(PDF_MAGIC):
        return _fail("not_pdf", "file is not a pdf")
    try:
        text = extractor.extract(data)
    except PdfUnreadable:
        return _fail("not_pdf", "pdf cannot be opened or read")

    # Control characters (\u0000 breaks Postgres jsonb) never reach the LLM or the result.
    stripped = strip_control(text).strip()
    text_chars = len(stripped)
    if text_chars < MIN_TEXT_CHARS:
        return _fail("no_text", "pdf has no extractable text", text_chars=text_chars)

    truncated = text_chars > MAX_LLM_CHARS
    llm_text = stripped[:MAX_LLM_CHARS]
    try:
        parsed = llm.parse(llm_text)
    except LlmRefused as exc:
        category = str(exc.args[0]) if exc.args else "unknown"
        return _fail(
            "llm_refused",
            f"llm refused: {category}",
            model_version=llm.model_version,
            text_chars=text_chars,
            truncated=truncated,
        )
    except LlmInvalidOutput as exc:
        code = str(exc.args[0]) if exc.args else "unknown"
        return _fail(
            "llm_invalid_output",
            f"llm output failed validation: {code}",
            model_version=llm.model_version,
            text_chars=text_chars,
            truncated=truncated,
        )

    parsed = parsed.model_copy(update={"skills": normalize(parsed.skills)})
    return ParseSuccess(
        parsed=parsed, model_version=llm.model_version, text_chars=text_chars, truncated=truncated
    )
