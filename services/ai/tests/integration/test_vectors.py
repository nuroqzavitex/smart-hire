import uuid
from collections.abc import Iterator

import pytest
from qdrant_client import QdrantClient

from ai_service.adapters.embedder import FakeEmbedder
from ai_service.adapters.vectors import QdrantVectors, VectorStoreUnavailable, collection_names

pytestmark = pytest.mark.integration


@pytest.fixture
def vectors(qdrant_url: str) -> Iterator[QdrantVectors]:
    client = QdrantClient(url=qdrant_url, timeout=5)
    version = f"test-{uuid.uuid4().hex[:8]}@768"
    v = QdrantVectors(client, version)
    v.ensure_collections()
    yield v
    for name in collection_names(version):
        client.delete_collection(name)


def test_collection_name_from_model_version() -> None:
    assert collection_names("gemini-embedding-001@768") == (
        "jobs_gemini_embedding_001_768",
        "cvs_gemini_embedding_001_768",
    )


def test_nearest_cvs_only_active(vectors: QdrantVectors) -> None:
    emb = FakeEmbedder()
    go, java, other = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    v_go, v_java, v_job = emb.embed(["golang backend kafka", "java spring", "golang backend developer"])
    vectors.upsert_cv(go, v_go, candidate_id=uuid.uuid4(), active=True)
    vectors.upsert_cv(java, v_java, candidate_id=uuid.uuid4(), active=True)
    vectors.upsert_cv(other, v_go, candidate_id=uuid.uuid4(), active=False)
    hits = vectors.nearest_cvs(v_job, limit=50)
    assert [h[0] for h in hits] == [go, java]
    assert hits[0][1] > hits[1][1]
    vectors.set_cv_active(active={other}, inactive={go})
    assert [h[0] for h in vectors.nearest_cvs(v_job, limit=1)] == [other]


def test_upsert_idempotent_get_and_delete(vectors: QdrantVectors) -> None:
    j = uuid.uuid4()
    (vec,) = FakeEmbedder().embed(["golang"])
    vectors.upsert_job(j, vec, recruiter_id=uuid.uuid4())
    vectors.upsert_job(j, vec, recruiter_id=uuid.uuid4())
    got = vectors.get_job_vector(j)
    assert got is not None
    assert got == pytest.approx(vec, abs=1e-6)
    assert [h[0] for h in vectors.nearest_jobs(vec, limit=50)] == [j]
    vectors.delete_job(j)
    vectors.delete_job(j)
    assert vectors.get_job_vector(j) is None
    assert vectors.get_cv_vector(uuid.uuid4()) is None


def test_unreachable_is_transient() -> None:
    v = QdrantVectors(QdrantClient(url="http://127.0.0.1:1", timeout=1), "x@768")
    with pytest.raises(VectorStoreUnavailable):
        v.ensure_collections()
