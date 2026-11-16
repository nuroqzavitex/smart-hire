import json
import time
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from conftest import BUCKET, dumps, new_cv, uploaded_event
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import Engine, text

pytestmark = pytest.mark.integration


def _put(minio_client: Any, key: str, data: bytes) -> None:
    import io

    minio_client.put_object(BUCKET, key, io.BytesIO(data), len(data), content_type="application/pdf")


def _schema(contracts_dir: Path, name: str) -> Any:
    return json.loads((contracts_dir / name).read_text(encoding="utf-8"))


def _validate(doc: Any, schema: Any) -> None:
    jsonschema.validate(doc, schema, format_checker=jsonschema.FormatChecker())


def _header(msg: Any, name: str) -> str:
    return dict(msg.headers() or []).get(name, b"").decode()


def test_cv_uploaded_produces_cv_parsed(
    worker: None,
    kafka: Any,
    minio_client: Any,
    cv_pdf: bytes,
    engine: Engine,
    spans: InMemorySpanExporter,
    contracts_dir: Path,
):
    cv = new_cv()
    _put(minio_client, cv.object_key, cv_pdf)
    tracer = trace.get_tracer("test")
    carrier: dict[str, str] = {}
    with tracer.start_as_current_span("test parent") as parent:
        inject(carrier)
        parent_ctx = parent.get_span_context()
    kafka.produce(
        "cv.uploaded",
        cv.cv_id.encode(),
        uploaded_event(cv),
        [("traceparent", carrier["traceparent"].encode())],
    )

    msgs = kafka.read_key("cv.parsed", cv.cv_id)
    assert len(msgs) == 1
    doc = json.loads(msgs[0].value())
    _validate(doc, _schema(contracts_dir, "cv.parsed.v1.json"))
    assert doc["payload"]["attempt_id"] == cv.attempt_id
    assert {"Go", "Kubernetes", "PostgreSQL"} <= set(doc["payload"]["parsed"]["skills"])
    assert doc["payload"]["model_version"] == "fake:rules-v1"
    assert doc["correlation_id"] == "corr-1"

    trace_hex = format(parent_ctx.trace_id, "032x")
    tp = _header(msgs[0], "traceparent").split("-")
    assert tp[1] == trace_hex
    assert tp[2] != format(parent_ctx.span_id, "016x")

    deadline = time.monotonic() + 10
    names: set[str] = set()
    while time.monotonic() < deadline:
        names = {
            s.name
            for s in spans.get_finished_spans()
            if s.context and s.context.trace_id == parent_ctx.trace_id
        }
        if {"cv.uploaded process", "cv.parsed publish"} <= names:
            break
        time.sleep(0.2)
    assert {"cv.uploaded process", "cv.parsed publish"} <= names

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT status FROM cv_parse_results WHERE cv_id = :c"), {"c": cv.cv_id}
        ).all()
        assert [r.status for r in row] == ["PARSED"]
        for _ in range(50):  # relay marks the row right after the publish returns
            unpublished = conn.execute(
                text("SELECT count(*) FROM outbox WHERE aggregate_id = :c AND published_at IS NULL"),
                {"c": cv.cv_id},
            ).scalar_one()
            if unpublished == 0:
                break
            conn.rollback()
            time.sleep(0.2)
        assert unpublished == 0


def test_duplicate_event_id_not_published_twice(worker: None, kafka: Any, minio_client: Any, cv_pdf: bytes):
    cv = new_cv()
    _put(minio_client, cv.object_key, cv_pdf)
    value = uploaded_event(cv)
    kafka.produce("cv.uploaded", cv.cv_id.encode(), value)
    kafka.produce("cv.uploaded", cv.cv_id.encode(), value)
    assert len(kafka.read_key("cv.parsed", cv.cv_id)) == 1
    time.sleep(10)
    assert len(kafka.read_key("cv.parsed", cv.cv_id, count=2, timeout=5)) == 1


def test_malformed_payload_goes_to_dlq(worker: None, kafka: Any):
    cv = new_cv()
    key = cv.cv_id.encode()
    missing_key = json.loads(uploaded_event(cv))
    del missing_key["payload"]["object_key"]
    kafka.produce("cv.uploaded", key, b"{}")
    kafka.produce("cv.uploaded", key, dumps(missing_key))
    msgs = kafka.read_key("cv.uploaded.dlq", cv.cv_id, count=2)
    assert len(msgs) == 2
    for m in msgs:
        assert _header(m, "dlq-original-topic") == "cv.uploaded"
    attempts = sorted(_header(m, "dlq-attempts") for m in msgs)
    assert attempts == ["0", "1"]


def test_not_pdf_produces_parse_failed(worker: None, kafka: Any, minio_client: Any, contracts_dir: Path):
    cv = new_cv()
    _put(minio_client, cv.object_key, b"<html>not a pdf</html>")
    kafka.produce("cv.uploaded", cv.cv_id.encode(), uploaded_event(cv))
    msgs = kafka.read_key("cv.parse_failed", cv.cv_id)
    assert len(msgs) == 1
    doc = json.loads(msgs[0].value())
    _validate(doc, _schema(contracts_dir, "cv.parse_failed.v1.json"))
    assert doc["payload"]["reason"] == "not_pdf"


def test_missing_object_produces_download_failed(worker: None, kafka: Any, contracts_dir: Path):
    cv = new_cv()
    kafka.produce("cv.uploaded", cv.cv_id.encode(), uploaded_event(cv))
    msgs = kafka.read_key("cv.parse_failed", cv.cv_id)
    assert len(msgs) == 1
    doc = json.loads(msgs[0].value())
    _validate(doc, _schema(contracts_dir, "cv.parse_failed.v1.json"))
    assert doc["payload"]["reason"] == "download_failed"
