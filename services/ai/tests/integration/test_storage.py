"""MinioObjectReader against the real MinIO image with the least-privilege ai-svc user.

Regression guard for E-R16 (region must be set, otherwise minio-py calls
GetBucketLocation, which ai-svc may not) and E-R9 (a missing key is NoSuchKey
even without s3:ListBucket).
"""

import io
from typing import Any
from uuid import uuid4

import pytest
from conftest import BUCKET
from minio import Minio
from minio.error import S3Error

from ai_service.adapters.storage import MinioObjectReader, StorageFault
from ai_service.services.cv_parser import MAX_CV_BYTES
from ai_service.services.cv_uploaded_handler import DownloadFailed, ObjectStat

pytestmark = pytest.mark.integration


def _client(container: Any, creds: tuple[str, str], region: str | None) -> Minio:
    endpoint = container.get_config()["endpoint"]
    return Minio(endpoint, access_key=creds[0], secret_key=creds[1], secure=False, region=region)


@pytest.fixture
def reader(minio_container: Any, ai_svc_credentials: tuple[str, str]) -> MinioObjectReader:
    client = _client(minio_container, ai_svc_credentials, "us-east-1")
    return MinioObjectReader(client, BUCKET, sleep=lambda _: None)


def _key() -> str:
    return f"cvs/{uuid4()}/{uuid4()}.pdf"


def _put(root: Any, key: str, data: bytes) -> None:
    root.put_object(BUCKET, key, io.BytesIO(data), len(data), content_type="application/pdf")


def test_least_privilege_stat_and_get(reader: MinioObjectReader, minio_client: Any):
    key, data = _key(), b"%PDF-1.7 least privilege"
    _put(minio_client, key, data)
    assert reader.stat(key) == ObjectStat(len(data), "application/pdf")
    assert reader.get(key) == data


@pytest.mark.parametrize("op", ["stat", "get"])
def test_least_privilege_missing_key_is_download_failed(reader: MinioObjectReader, op: str):
    with pytest.raises(DownloadFailed) as exc:
        getattr(reader, op)(_key())
    assert not isinstance(exc.value, StorageFault)
    assert "cvs/" not in str(exc.value)


def test_least_privilege_get_is_bounded(reader: MinioObjectReader, minio_client: Any):
    key = _key()
    _put(minio_client, key, b"%PDF-" + b"x" * (MAX_CV_BYTES + 10))
    assert len(reader.get(key)) == MAX_CV_BYTES + 1


def test_least_privilege_get_empty_object(reader: MinioObjectReader, minio_client: Any):
    key = _key()
    _put(minio_client, key, b"")
    assert reader.get(key) == b""


def test_policy_is_read_only(minio_container: Any, ai_svc_credentials: tuple[str, str]):
    client = _client(minio_container, ai_svc_credentials, "us-east-1")
    with pytest.raises(S3Error) as exc:
        client.put_object(BUCKET, _key(), io.BytesIO(b"x"), 1)
    assert exc.value.code == "AccessDenied"


def test_without_region_is_storage_fault(
    minio_container: Any, ai_svc_credentials: tuple[str, str], minio_client: Any
):
    # Without a region minio-py asks GetBucketLocation, which ai-svc may not (E-R16).
    key = _key()
    _put(minio_client, key, b"%PDF-1.7")
    reader = MinioObjectReader(
        _client(minio_container, ai_svc_credentials, None), BUCKET, sleep=lambda _: None
    )
    with pytest.raises(StorageFault):
        reader.stat(key)
