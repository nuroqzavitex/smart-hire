"""Qdrant vector store of job and CV embeddings (ADR 0010).

Collections are named after the model version, so a new model writes to new
collections (spec R5). Point ids are the job/CV UUIDs, which makes every write
idempotent; transport failures become VectorStoreUnavailable (transient).
"""

import re
from collections.abc import Callable, Iterable
from typing import Any
from uuid import UUID

import httpx
from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from ai_service.adapters.embedder import DIM


class VectorStoreUnavailable(Exception):
    """Transient: Qdrant unreachable, timed out or answered 5xx."""


def collection_names(model_version: str) -> tuple[str, str]:
    slug = re.sub(r"[^a-z0-9]+", "_", model_version.lower()).strip("_")
    return f"jobs_{slug}", f"cvs_{slug}"


def _guard[T](fn: Callable[[], T]) -> T:
    try:
        return fn()
    except UnexpectedResponse as exc:
        if exc.status_code is not None and exc.status_code >= 500:
            raise VectorStoreUnavailable(f"qdrant status {exc.status_code}") from None
        raise
    except (ResponseHandlingException, httpx.TransportError, OSError) as exc:
        raise VectorStoreUnavailable(type(exc).__name__) from None


class QdrantVectors:
    def __init__(self, client: QdrantClient, model_version: str) -> None:
        self._client = client
        self.jobs, self.cvs = collection_names(model_version)

    def ensure_collections(self) -> None:
        def run() -> None:
            for name in (self.jobs, self.cvs):
                if not self._client.collection_exists(name):
                    self._client.create_collection(
                        name, vectors_config=models.VectorParams(size=DIM, distance=models.Distance.COSINE)
                    )
            self._client.create_payload_index(self.cvs, "active", models.PayloadSchemaType.BOOL)

        _guard(run)

    def upsert_job(self, job_id: UUID, vector: list[float], *, recruiter_id: UUID) -> None:
        point = models.PointStruct(id=str(job_id), vector=vector, payload={"recruiter_id": str(recruiter_id)})
        _guard(lambda: self._client.upsert(self.jobs, [point], wait=True))

    def upsert_cv(self, cv_id: UUID, vector: list[float], *, candidate_id: UUID, active: bool) -> None:
        payload = {"candidate_id": str(candidate_id), "active": active}
        point = models.PointStruct(id=str(cv_id), vector=vector, payload=payload)
        _guard(lambda: self._client.upsert(self.cvs, [point], wait=True))

    def set_cv_active(self, *, active: Iterable[UUID], inactive: Iterable[UUID]) -> None:
        for ids, value in ((list(inactive), False), (list(active), True)):
            if ids:
                self._set_active([str(i) for i in ids], value)

    def _set_active(self, ids: list[str], value: bool) -> None:
        selector: Any = ids
        _guard(lambda: self._client.set_payload(self.cvs, {"active": value}, selector, wait=True))

    def delete_job(self, job_id: UUID) -> None:
        self._delete(self.jobs, job_id)

    def delete_cv(self, cv_id: UUID) -> None:
        self._delete(self.cvs, cv_id)

    def _delete(self, collection: str, point_id: UUID) -> None:
        selector = models.PointIdsList(points=[str(point_id)])
        _guard(lambda: self._client.delete(collection, selector, wait=True))

    def get_job_vector(self, job_id: UUID) -> list[float] | None:
        return self._vector(self.jobs, job_id)

    def get_cv_vector(self, cv_id: UUID) -> list[float] | None:
        return self._vector(self.cvs, cv_id)

    def _vector(self, collection: str, point_id: UUID) -> list[float] | None:
        recs = _guard(lambda: self._client.retrieve(collection, [str(point_id)], with_vectors=True))
        if not recs or recs[0].vector is None:
            return None
        vec = recs[0].vector
        if not isinstance(vec, list) or not all(isinstance(x, float | int) for x in vec):
            return None
        return [float(x) for x in vec]  # type: ignore[arg-type]

    def nearest_cvs(self, vector: list[float], limit: int) -> list[tuple[UUID, float]]:
        flt = models.Filter(must=[models.FieldCondition(key="active", match=models.MatchValue(value=True))])
        return self._nearest(self.cvs, vector, limit, flt)

    def nearest_jobs(self, vector: list[float], limit: int) -> list[tuple[UUID, float]]:
        return self._nearest(self.jobs, vector, limit, None)

    def _nearest(
        self, collection: str, vector: list[float], limit: int, flt: models.Filter | None
    ) -> list[tuple[UUID, float]]:
        resp = _guard(
            lambda: self._client.query_points(
                collection, query=vector, query_filter=flt, limit=limit, with_payload=False
            )
        )
        return [(UUID(str(p.id)), float(p.score)) for p in resp.points]
