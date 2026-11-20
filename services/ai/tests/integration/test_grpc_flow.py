import uuid
from collections.abc import Iterator
from typing import Any

import grpc
import pytest
from qdrant_client import QdrantClient
from sqlalchemy import Engine
from test_matcher_flow import NOW, cv_event, job_event

from ai_service.adapters.embedder import FakeEmbedder
from ai_service.adapters.grpc_server import AIServicer, build_server
from ai_service.adapters.match_store import PgMatchStore
from ai_service.adapters.vectors import QdrantVectors, collection_names
from ai_service.services.matcher import MatchHandler
from smarthire.ai.v1 import ai_pb2, ai_pb2_grpc

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_matching")]


@pytest.fixture
def flow(engine: Engine, qdrant_url: str) -> Iterator[tuple[MatchHandler, ai_pb2_grpc.AIServiceStub]]:
    client = QdrantClient(url=qdrant_url, timeout=5)
    emb = FakeEmbedder()
    vectors = QdrantVectors(client, emb.model_version)
    vectors.ensure_collections()
    store = PgMatchStore(engine)
    handler = MatchHandler(store, vectors, emb, clock=lambda: NOW, trace_id=lambda: "t")
    server, port = build_server(AIServicer(store, vectors, emb.model_version), "127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield handler, ai_pb2_grpc.AIServiceStub(channel)
    finally:
        channel.close()
        server.stop(0)
        for name in collection_names(emb.model_version):
            client.delete_collection(name)


def test_events_to_grpc(flow: Any) -> None:
    handler, stub = flow
    cv, cand, job = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    handler.handle(cv_event(cv, cand))
    handler.handle(job_event(job))

    found = stub.FindCandidates(ai_pb2.FindCandidatesRequest(job_id=str(job)))
    assert [(c.cv_id, c.candidate_id) for c in found.candidates] == [(str(cv), str(cand))]
    assert found.candidates[0].score > 40

    rec = stub.RecommendJobs(ai_pb2.RecommendJobsRequest(cv_id=str(cv)))
    assert [j.job_id for j in rec.jobs] == [str(job)]
    assert rec.jobs[0].score == pytest.approx(found.candidates[0].score)

    one = stub.ScoreMatch(ai_pb2.ScoreMatchRequest(cv_id=str(cv), job_id=str(job)))
    assert one.score == pytest.approx(found.candidates[0].score, abs=0.01)
    assert list(one.breakdown.matched_skills) == ["Go", "Kafka"]
