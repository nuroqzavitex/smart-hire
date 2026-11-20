"""Embedding port (ADR 0010). Exception arguments never carry the embedded text."""

from collections.abc import Sequence
from typing import Protocol


class EmbeddingUnavailable(Exception):
    """Transient: rate limited (429), server error or network failure; retry later."""


class Embedder(Protocol):
    @property
    def model_version(self) -> str: ...

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """One L2-normalised vector per text, in order."""
        ...
