"""Integration fixtures: Postgres, Redpanda and the locally built MinIO image (E-R4/E-R13a)."""

import json
import logging
import os
import threading
import time
import warnings
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import confluent_kafka
import pytest
from alembic import command
from alembic.config import Config
from confluent_kafka.admin import AdminClient, NewTopic
from minio.credentials import StaticProvider
from minio.minioadmin import MinioAdmin
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pdfs import pdf
from sqlalchemy import Engine, create_engine, text
from ulid import ULID

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    from testcontainers.kafka import RedpandaContainer
    from testcontainers.minio import MinioContainer
    from testcontainers.postgres import PostgresContainer

from ai_service.adapters.db import PgResultStore, make_engine
from ai_service.adapters.kafka_consumer import RetryingConsumer
from ai_service.adapters.kafka_producer import KafkaPublisher
from ai_service.adapters.llm import FakeLlmParser
from ai_service.adapters.outbox import OutboxRelay
from ai_service.adapters.pdf import PyMuPdfExtractor
from ai_service.adapters.storage import MinioObjectReader
from ai_service.domain.events import dump_envelope, new_envelope
from ai_service.services.cv_uploaded_handler import CvUploadedHandler

# The ryuk reaper image is not pulled (offline-friendly); the fixtures stop their containers.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

# Built by `make minio-image` (deploy/docker/minio/Dockerfile), not pulled from Docker Hub.
MINIO_IMAGE = "smarthire/minio:RELEASE.2025-04-22T22-12-26Z"
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"
POSTGRES_IMAGE = "postgres:16-alpine"
MINIO_USER, MINIO_PASSWORD = "minioadmin", "minioadmin-secret"
BUCKET = "cvs"
TOPICS = ["cv.uploaded", "cv.parsed", "cv.parse_failed", "relay.test"]
ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
AI_SVC_POLICY = Path(__file__).resolve().parents[4] / "deploy" / "docker" / "init" / "minio" / "ai-svc.json"


@pytest.fixture(scope="session", autouse=True)
def spans() -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return exporter


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        container = PostgresContainer(POSTGRES_IMAGE, driver="psycopg")
    with container as pg:
        admin = create_engine(pg.get_connection_url(), isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            conn.execute(text("CREATE ROLE ai_svc LOGIN NOSUPERUSER PASSWORD 'ai_svc_pw'"))
            conn.execute(text("CREATE DATABASE ai_db OWNER ai_svc"))
        admin.dispose()
        dsn = (
            f"postgresql+psycopg://ai_svc:ai_svc_pw@{pg.get_container_host_ip()}:"
            f"{pg.get_exposed_port(5432)}/ai_db"
        )
        os.environ["AI_DB_DSN"] = dsn
        command.upgrade(Config(str(ALEMBIC_INI)), "head")
        eng = make_engine(dsn)
        yield eng
        eng.dispose()


@dataclass
class Kafka:
    bootstrap: str

    def produce(
        self, topic: str, key: bytes, value: bytes, headers: list[tuple[str, bytes]] | None = None
    ) -> None:
        producer = confluent_kafka.Producer({"bootstrap.servers": self.bootstrap})
        producer.produce(topic, key=key, value=value, headers=headers or [])
        assert producer.flush(15) == 0

    def read(
        self,
        topic: str,
        match: Callable[[confluent_kafka.Message], bool],
        *,
        count: int = 1,
        timeout: float = 30,
    ) -> list[confluent_kafka.Message]:
        """Reads `topic` from the start until `count` records satisfy `match` (or timeout)."""
        consumer = confluent_kafka.Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"test-{uuid4()}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        found: list[confluent_kafka.Message] = []
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline and len(found) < count:
                msg = consumer.poll(1.0)
                if msg is not None and not msg.error() and match(msg):
                    found.append(msg)
        finally:
            consumer.close()
        return found

    def read_key(self, topic: str, key: str, **kw: Any) -> list[confluent_kafka.Message]:
        return self.read(topic, lambda m: m.key() == key.encode(), **kw)


@pytest.fixture(scope="session")
def kafka() -> Iterator[Kafka]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        container = RedpandaContainer(REDPANDA_IMAGE)
    with container as rp:
        bootstrap = rp.get_bootstrap_server()
        admin = AdminClient({"bootstrap.servers": bootstrap})
        topics = [*TOPICS, *(f"{t}.dlq" for t in TOPICS)]
        for name, fut in admin.create_topics(
            [NewTopic(t, num_partitions=3, replication_factor=1) for t in topics]
        ).items():
            fut.result(30)
            logging.getLogger(__name__).info("created topic %s", name)
        yield Kafka(bootstrap)


@pytest.fixture(scope="session")
def minio_container() -> Iterator[MinioContainer]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        container = MinioContainer(MINIO_IMAGE, access_key=MINIO_USER, secret_key=MINIO_PASSWORD)
    with container as mc:
        mc.get_client().make_bucket(BUCKET)
        yield mc


@pytest.fixture(scope="session")
def minio_client(minio_container: MinioContainer) -> Any:
    """Root client: test setup only (puts objects)."""
    return minio_container.get_client()


@pytest.fixture(scope="session")
def ai_svc_credentials(minio_container: MinioContainer) -> tuple[str, str]:
    """A least-privilege user with the repo's ai-svc policy, as minio-init sets it up."""
    endpoint = minio_container.get_config()["endpoint"]
    admin = MinioAdmin(
        endpoint=endpoint, credentials=StaticProvider(MINIO_USER, MINIO_PASSWORD), secure=False
    )
    access, secret = "ai-svc-test", "ai-svc-test-secret"
    admin.policy_add("ai-svc", policy_file=str(AI_SVC_POLICY))
    admin.user_add(access, secret)
    admin.attach_policy(["ai-svc"], user=access)
    return access, secret


@pytest.fixture
def cv_pdf() -> bytes:
    return pdf(["Nguyen Van Nam", "nguyen.nam@example.com", "Skills: golang, k8s, postgres", "x" * 60])


@dataclass
class UploadedCv:
    cv_id: str
    candidate_id: str
    attempt_id: str
    object_key: str


def new_cv() -> UploadedCv:
    cand, cv = uuid4(), uuid4()
    return UploadedCv(str(cv), str(cand), str(ULID()), f"cvs/{cand}/{cv}.pdf")


def uploaded_event(cv: UploadedCv, **payload_override: Any) -> bytes:
    payload = {
        "cv_id": cv.cv_id,
        "candidate_id": cv.candidate_id,
        "object_key": cv.object_key,
        "attempt_id": cv.attempt_id,
    }
    payload.update(payload_override)
    env = new_envelope(
        "cv.uploaded", payload, now=datetime.now(UTC), trace_id="0" * 32, correlation_id="corr-1"
    )
    return dump_envelope(env.model_copy(update={"source": "candidate-service"}))


@pytest.fixture
def worker(engine: Engine, kafka: Kafka, minio_client: Any) -> Iterator[None]:
    """Consumer + relay on their own threads with a fresh consumer group, stopped after the test."""
    stop = threading.Event()
    publisher = KafkaPublisher(kafka.bootstrap)
    handler = CvUploadedHandler(
        MinioObjectReader(minio_client, BUCKET),
        PyMuPdfExtractor(),
        FakeLlmParser(),
        PgResultStore(engine),
        clock=lambda: datetime.now(UTC),
        trace_id=lambda: format(trace.get_current_span().get_span_context().trace_id, "032x"),
    )
    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": kafka.bootstrap,
            "group.id": f"ai-test-{uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["cv.uploaded"])
    log = logging.getLogger("worker-test")
    rc = RetryingConsumer(consumer, publisher, handler.handle, stop, log=log)  # type: ignore[arg-type]
    relay = OutboxRelay(engine, publisher, interval_s=0.1, stop=stop, log=log)
    threads = [threading.Thread(target=rc.run), threading.Thread(target=relay.run)]
    for t in threads:
        t.start()
    try:
        yield
    finally:
        stop.set()
        for t in threads:
            t.join(15)
        consumer.close()


def dumps(obj: Any) -> bytes:
    return json.dumps(obj).encode()
