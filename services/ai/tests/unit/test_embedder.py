import math
from typing import Any

import pytest
from google.genai import errors

from ai_service.adapters.embedder import DIM, FakeEmbedder, GeminiEmbedder
from ai_service.services.embedding import EmbeddingUnavailable


def _cos(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b, strict=True))


def test_fake_is_deterministic_and_normalized() -> None:
    e = FakeEmbedder()
    a, b = e.embed(["Golang backend", "Golang backend"])
    assert a == b
    assert len(a) == DIM
    assert math.isclose(math.sqrt(sum(x * x for x in a)), 1.0, rel_tol=1e-9)
    assert e.model_version == "fake-hash@768"


def test_fake_similar_texts_are_closer() -> None:
    a, b, c = FakeEmbedder().embed(["golang backend", "Backend Golang developer", "chef pastry kitchen"])
    assert _cos(a, b) > _cos(a, c)


def test_fake_ignores_accents_and_empty_is_zero() -> None:
    a, b, empty = FakeEmbedder().embed(["Hà Nội", "ha noi", ""])
    assert a == b
    assert all(x == 0.0 for x in empty)


class _Emb:
    def __init__(self, values: list[float]) -> None:
        self.values = values


class _Resp:
    def __init__(self, n: int) -> None:
        self.embeddings = [_Emb([3.0, 4.0] + [0.0] * (DIM - 2)) for _ in range(n)]


class _Models:
    def __init__(self, fail: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail = fail

    def embed_content(self, **kw: Any) -> _Resp:
        self.calls.append(kw)
        if self.fail:
            raise self.fail
        return _Resp(len(kw["contents"]))


class _Client:
    def __init__(self, models: _Models) -> None:
        self.models = models


def test_gemini_batches_and_normalizes() -> None:
    models = _Models()
    g = GeminiEmbedder(_Client(models), "gemini-embedding-001")
    vecs = g.embed([f"t{i}" for i in range(150)])
    assert len(vecs) == 150
    assert [len(c["contents"]) for c in models.calls] == [100, 50]
    cfg = models.calls[0]["config"]
    assert cfg.task_type == "RETRIEVAL_DOCUMENT"
    assert cfg.output_dimensionality == DIM
    assert models.calls[0]["model"] == "gemini-embedding-001"
    assert vecs[0][:2] == pytest.approx([0.6, 0.8])
    assert g.model_version == "gemini-embedding-001@768"


@pytest.mark.parametrize("code", [429, 500, 503])
def test_gemini_transient_errors(code: int) -> None:
    err = errors.APIError(code, {"error": {"message": "x", "status": "UNAVAILABLE"}})
    with pytest.raises(EmbeddingUnavailable):
        GeminiEmbedder(_Client(_Models(err)), "m").embed(["a"])


def test_gemini_client_error_is_raised() -> None:
    err = errors.APIError(400, {"error": {"message": "bad", "status": "INVALID_ARGUMENT"}})
    with pytest.raises(errors.APIError):
        GeminiEmbedder(_Client(_Models(err)), "m").embed(["a"])


def test_gemini_network_error_is_transient() -> None:
    with pytest.raises(EmbeddingUnavailable):
        GeminiEmbedder(_Client(_Models(ConnectionError("down"))), "m").embed(["a"])


def test_gemini_empty_input_no_call() -> None:
    models = _Models()
    assert GeminiEmbedder(_Client(models), "m").embed([]) == []
    assert models.calls == []


@pytest.mark.parametrize(
    ("code", "body"),
    [
        (401, {"error": {"message": "unauthenticated", "status": "UNAUTHENTICATED"}}),
        (403, {"error": {"message": "permission denied", "status": "PERMISSION_DENIED"}}),
        (
            400,
            {
                "error": {
                    "message": "API key not valid. Please pass a valid API key.",
                    "status": "INVALID_ARGUMENT",
                    "details": [{"reason": "API_KEY_INVALID"}],
                }
            },
        ),
    ],
)
def test_gemini_key_problems_are_transient(code: int, body: dict[str, Any]) -> None:
    # A bad or revoked key must stall the matcher, not dead-letter every event.
    with pytest.raises(EmbeddingUnavailable):
        GeminiEmbedder(_Client(_Models(errors.APIError(code, body))), "m").embed(["a"])
