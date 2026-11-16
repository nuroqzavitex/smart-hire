"""MinIO object reader with bounded retries (plan decision #5, E-R9/E-R13b).

Raised errors never carry the object key or request URL.
"""

import logging
import time
from collections.abc import Callable
from typing import Any

from minio import Minio
from minio.error import S3Error

from ai_service.adapters.kafka_consumer import backoff
from ai_service.services.cv_parser import MAX_CV_BYTES
from ai_service.services.cv_uploaded_handler import DownloadFailed, ObjectStat

log = logging.getLogger(__name__)

_NOT_FOUND = "NoSuchKey"
# A ranged GET of a 0-byte object is unsatisfiable: the object is empty.
_EMPTY_RANGE = "InvalidRange"
# Deployment faults (wrong policy / bucket): not a per-CV problem, so no adapter
# retry and no cv.parse_failed; the consumer retries and then dead-letters.
_DEPLOYMENT_FAULTS = frozenset({"AccessDenied", "NoSuchBucket"})


class StorageFault(Exception):
    """Unexpected, non-retryable storage error (misconfiguration)."""


class MinioObjectReader:
    def __init__(
        self,
        client: Minio,
        bucket: str,
        max_attempts: int = 3,
        base_s: float = 0.2,
        cap_s: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        max_bytes: int = MAX_CV_BYTES + 1,
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._max_attempts = max_attempts
        self._base_s = base_s
        self._cap_s = cap_s
        self._sleep = sleep
        self._max_bytes = max_bytes

    def stat(self, key: str) -> ObjectStat:
        def call() -> ObjectStat:
            info = self._client.stat_object(self._bucket, key)
            return ObjectStat(size=int(info.size or 0), content_type=str(info.content_type or ""))

        return self._with_retry(call)

    def get(self, key: str) -> bytes:
        """Reads at most max_bytes (default MAX_CV_BYTES + 1).

        The presigned PUT URL outlives complete, so the object may have been
        replaced after stat; reading one byte past the limit lets the caller
        detect an oversized object without downloading it unbounded.
        """

        def call() -> bytes:
            try:
                response = self._client.get_object(self._bucket, key, length=self._max_bytes)
            except S3Error as exc:
                if exc.code == _EMPTY_RANGE:
                    return b""
                raise
            try:
                data: bytes = response.read()
            finally:
                response.close()
                response.release_conn()
            return data

        return self._with_retry(call)

    def _with_retry[T](self, call: Callable[[], T]) -> T:
        attempt = 0
        while True:
            attempt += 1
            try:
                return call()
            except S3Error as exc:
                # `from None`: the S3Error carries the resource path (object key).
                code = exc.code
                if code == _NOT_FOUND:
                    raise DownloadFailed("object not found") from None
                if code in _DEPLOYMENT_FAULTS:
                    raise StorageFault(f"storage rejected the request: {code}") from None
                status: Any = getattr(exc.response, "status", 0)
                if not isinstance(status, int) or status < 500:
                    raise StorageFault(f"storage request failed: {code}") from None
            except Exception as exc:
                # Network errors (urllib3, OSError) are transient; their text may hold the URL.
                log.debug("transient storage error", extra={"error_type": type(exc).__name__})
            if attempt >= self._max_attempts:
                raise DownloadFailed("object download failed after retries") from None
            self._sleep(backoff(attempt, self._base_s, self._cap_s))
