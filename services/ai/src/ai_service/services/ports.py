"""Ports of the CV parse use case. Exception arguments never carry PII."""

from typing import Protocol

from ai_service.domain.models import ParsedCV


class LlmRefused(Exception):
    """The model refused; args[0] is the refusal category or "unknown"."""


class LlmInvalidOutput(Exception):
    """No usable output; args[0] is a short code:
    "no_parsed_output" | "validation" | "api_status_<code>"."""


class PdfUnreadable(Exception):
    """The bytes cannot be opened or read as a PDF (corrupt, encrypted, ...)."""


class LlmParser(Protocol):
    @property
    def model_version(self) -> str: ...

    def parse(self, cv_text: str) -> ParsedCV: ...


class TextExtractor(Protocol):
    def extract(self, data: bytes) -> str:
        """Returns the text layer; raises PdfUnreadable when it cannot be opened/read."""
        ...
