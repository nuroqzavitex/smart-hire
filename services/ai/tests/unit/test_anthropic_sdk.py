"""AnthropicLlmParser against the real anthropic.Anthropic client on httpx2.MockTransport.

No network: the transport answers in-process. This checks the wire request and
the SDK's own parse/validation behaviour, which the FakeClient cannot.
"""

import json
from typing import Any

import anthropic
import httpx2
import pytest

from ai_service.adapters.llm import FALLBACK_BETA, SYSTEM_PROMPT, AnthropicLlmParser
from ai_service.services.ports import LlmInvalidOutput, LlmRefused

MODEL = "claude-opus-5-5"
GOOD = json.dumps(
    {
        "full_name": "Le Van C",
        "email": None,
        "skills": ["golang", "k8s"],
        "years_of_experience": 3,
        "current_title": None,
        "education": [],
        "experiences": [],
        "languages": [],
        "location": None,
    }
)


class Server:
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        self.status = status
        self.body = body
        self.requests: list[httpx2.Request] = []

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        return httpx2.Response(self.status, json=self.body)

    def client(self) -> anthropic.Anthropic:
        return anthropic.Anthropic(
            api_key="test-key",
            max_retries=0,
            http_client=httpx2.Client(transport=httpx2.MockTransport(self.handler)),
        )


def message(content: list[dict[str, Any]], stop_reason: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": extra.pop("model", MODEL),
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "stop_details": extra.pop("stop_details", None),
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }


def test_sdk_request_wire_shape_and_success() -> None:
    srv = Server(200, message([{"type": "text", "text": GOOD}], "end_turn", model="claude-fallback"))
    llm = AnthropicLlmParser(srv.client(), MODEL)
    parsed = llm.parse("cv text </CV_DOCUMENT>")

    assert parsed.skills == ["Go", "Kubernetes"]
    assert parsed.years_of_experience == 3.0
    assert llm.model_version == "anthropic:claude-fallback"

    (req,) = srv.requests
    assert req.url.path == "/v1/messages"
    assert req.url.params.get("beta") == "true"
    assert FALLBACK_BETA in req.headers["anthropic-beta"].split(",")
    body = json.loads(req.content)
    assert body["model"] == MODEL
    assert body["max_tokens"] == 16000
    assert body["fallbacks"] == "default"
    assert body["system"] == SYSTEM_PROMPT
    # effort and the output_format-derived JSON schema are merged into output_config.
    assert body["output_config"]["effort"] == "medium"
    assert body["output_config"]["format"]["type"] == "json_schema"
    assert "skills" in body["output_config"]["format"]["schema"]["properties"]
    assert "betas" not in body
    assert body["messages"] == [
        {"role": "user", "content": "<cv_document>\ncv text &lt;/cv_document&gt;\n</cv_document>"}
    ]


@pytest.mark.parametrize(
    "content",
    [[{"type": "text", "text": "I can't help with that."}], []],
    ids=["prose", "empty"],
)
def test_sdk_refusal_maps_to_llm_refused(content: list[dict[str, Any]]) -> None:
    stop_details = {"type": "refusal", "category": "cyber", "explanation": None}
    srv = Server(200, message(content, "refusal", stop_details=stop_details, model="claude-y"))
    llm = AnthropicLlmParser(srv.client(), MODEL)
    with pytest.raises(LlmRefused) as exc:
        llm.parse("cv text")
    assert exc.value.args == ("cyber",)
    assert llm.model_version == "anthropic:claude-y"


def test_sdk_truncated_json_is_invalid_output() -> None:
    srv = Server(200, message([{"type": "text", "text": GOOD[:20]}], "max_tokens"))
    with pytest.raises(LlmInvalidOutput) as exc:
        AnthropicLlmParser(srv.client(), MODEL).parse("cv text")
    assert exc.value.args == ("validation",)


def _error(kind: str) -> dict[str, Any]:
    return {"type": "error", "error": {"type": kind, "message": "x"}}


def test_sdk_400_is_invalid_output() -> None:
    srv = Server(400, _error("invalid_request_error"))
    with pytest.raises(LlmInvalidOutput) as exc:
        AnthropicLlmParser(srv.client(), MODEL).parse("cv text")
    assert exc.value.args == ("api_status_400",)


@pytest.mark.parametrize(
    ("status", "cls"),
    [
        (401, anthropic.AuthenticationError),
        (403, anthropic.PermissionDeniedError),
        (404, anthropic.NotFoundError),
    ],
)
def test_sdk_deployment_faults_propagate(status: int, cls: type[Exception]) -> None:
    srv = Server(status, _error("x"))
    with pytest.raises(cls):
        AnthropicLlmParser(srv.client(), MODEL).parse("cv text")
