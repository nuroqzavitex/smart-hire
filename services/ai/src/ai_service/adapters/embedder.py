"""Embedder adapters: a deterministic fake (dev/CI/smoke) and Gemini (ADR 0010)."""

import hashlib
import math
import re
from collections.abc import Sequence
from typing import Any

from google import genai
from google.genai import errors, types

from ai_service.config import Settings
from ai_service.domain.skills import unaccent
from ai_service.services.embedding import Embedder, EmbeddingUnavailable

DIM = 768
GEMINI_BATCH = 100
_TOKEN = re.compile(r"[a-z0-9+#.]+")


def _normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vec))
    return [x / norm for x in vec] if norm else vec


class FakeEmbedder:
    """Hashed bag of words: texts sharing (accent-folded) words get close vectors."""

    model_version = f"fake-hash@{DIM}"

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [self._one(t) for t in texts]

    @staticmethod
    def _one(text: str) -> list[float]:
        vec = [0.0] * DIM
        for token in _TOKEN.findall(unaccent(text).casefold()):
            digest = hashlib.sha256(token.encode()).digest()
            bucket = int.from_bytes(digest[:4], "big") % DIM
            vec[bucket] += 1.0 if digest[4] & 1 else -1.0
        return _normalize(vec)


class GeminiEmbedder:
    def __init__(self, client: Any, model: str) -> None:
        self._client = client
        self._model = model
        self.model_version = f"{model}@{DIM}"

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for i in range(0, len(texts), GEMINI_BATCH):
            out.extend(self._batch(list(texts[i : i + GEMINI_BATCH])))
        return out

    def _batch(self, texts: list[str]) -> list[list[float]]:
        config = types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT", output_dimensionality=DIM)
        try:
            resp = self._client.models.embed_content(model=self._model, contents=texts, config=config)
        except errors.APIError as exc:
            if _transient(exc):
                raise EmbeddingUnavailable(f"gemini status {exc.code}") from None
            raise
        except (ConnectionError, TimeoutError, OSError) as exc:
            raise EmbeddingUnavailable(type(exc).__name__) from None
        except Exception as exc:  # httpx transport errors and timeouts
            if type(exc).__module__.startswith("httpx"):
                raise EmbeddingUnavailable(type(exc).__name__) from None
            raise
        return [_normalize(list(e.values)) for e in resp.embeddings]


def _transient(exc: errors.APIError) -> bool:
    """Rate limit, server error or a key problem: retrying later can succeed (a bad or
    revoked key stalls the matcher until it is fixed instead of dead-lettering every event)."""
    if exc.code in (401, 403, 429) or exc.code >= 500:
        return True
    return exc.code == 400 and "API_KEY" in f"{exc.details}{exc.message}"


def build_embedder(settings: Settings) -> Embedder:
    if settings.embedding_provider == "gemini":
        client = genai.Client(
            api_key=settings.gemini_api_key.get_secret_value(),
            http_options=types.HttpOptions(
                timeout=int(settings.embedding_timeout_s * 1000),
                retry_options=types.HttpRetryOptions(attempts=1),
            ),
        )
        return GeminiEmbedder(client, settings.embedding_model)
    return FakeEmbedder()
