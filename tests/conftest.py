# pyright: reportWildcardImportFromLibrary=false

import os
import re
import subprocess
import timeit
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from pathlib import Path
from typing import Any

import anyio
import grpc
import pytest
from anyio.to_thread import run_sync
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.pubsub_v1 import PublisherClient
from google.pubsub_v1.services.publisher.transports import PublisherGrpcTransport

from mersal.gcp_pubsub.subscription_storage import (
    GCPPubSubSubscriptionStorage,
    GCPPubSubSubscriptionStorageConfig,
)
from mersal.gcp_pubsub.transport import GCPPubSubTransport, GCPPubSubTransportConfig
from mersal.logging import NullLogger
from mersal.subscription import SubscriptionStorage
from mersal.testing.core._internal.conftest import *
from mersal.testing.core.subscription.basic_subscription_storage_tests import SubscriptionStorageMaker
from mersal.testing.core.transport.basic_transport_tests import TransportMaker
from mersal.transport import Transport
from mersal.utils.sync import AsyncCallable

__all__ = (
    "DockerServiceRegistry",
    "docker_ip",
    "docker_services",
    "event_topic_prefix",
    "gcp_pubsub_service",
    "gcp_pubsub_subscription_storage_maker",
    "gcp_pubsub_transport_maker",
    "project_id",
    "wait_until_responsive",
)


class DockerServiceTimeoutError(Exception):
    """Timeout error."""


async def wait_until_responsive(
    check: Callable[..., Awaitable],
    timeout: float,
    pause: float,
    **kwargs: Any,
) -> None:
    """Wait until a service is responsive.

    Args:
        check: Coroutine, return truthy value when waiting should stop.
        timeout: Maximum seconds to wait.
        pause: Seconds to wait between calls to `check`.
        **kwargs: Given as kwargs to `check`.
    """
    ref = timeit.default_timer()
    now = ref
    while (now - ref) < timeout:
        if await check(**kwargs):
            return
        await anyio.sleep(pause)
        now = timeit.default_timer()

    raise DockerServiceTimeoutError("Timeout reached while waiting on service!")


class DockerServiceRegistry:
    def __init__(self) -> None:
        self._running_services: set[str] = set()
        self.docker_ip = self._get_docker_ip()
        file_name = Path(__file__).resolve().parent / "docker-compose.yml"
        self._base_command = [
            "docker",
            "compose",
            f"--file={file_name!s}",
            "--project-name=mersal_gcp_pubsub_pytest",
        ]

    def _get_docker_ip(self) -> str:
        docker_host = os.environ.get("DOCKER_HOST", "").strip()
        if not docker_host or docker_host.startswith("unix://"):
            return "127.0.0.1"

        match = re.match(r"^tcp://(.+?):\d+$", docker_host)
        if not match:
            raise ValueError(f'Invalid value for DOCKER_HOST: "{docker_host}".')
        return match.group(1)

    def run_command(self, *args: str) -> None:
        subprocess.run([*self._base_command, *args], check=True, capture_output=True)

    async def start(
        self,
        name: str,
        *,
        check: Callable[..., Awaitable],
        timeout: float = 30,
        pause: float = 0.1,
        **kwargs: Any,
    ) -> None:
        if name not in self._running_services:
            self.run_command("up", "-d", name)
            self._running_services.add(name)

            await wait_until_responsive(
                check=AsyncCallable(check),
                timeout=timeout,
                pause=pause,
                host=self.docker_ip,
                **kwargs,
            )

    def stop(self, name: str) -> None:
        pass

    def down(self) -> None:
        self.run_command("down", "-t", "5")


@pytest.fixture(scope="session")
def docker_services() -> Generator[DockerServiceRegistry, None, None]:
    registry = DockerServiceRegistry()
    yield registry
    registry.down()


@pytest.fixture(scope="session")
def docker_ip(docker_services: DockerServiceRegistry) -> str:
    return docker_services.docker_ip


async def pubsub_emulator_responsive(host: str, port: int = 8085, timeout: float = 5.0) -> bool:
    """Checks readiness with an actual Pub/Sub API call rather than a bare TCP connect -
    the emulator's port can start accepting connections slightly before its Pub/Sub
    service is actually ready to serve requests.
    """

    def _check() -> bool:
        # `client_options={"api_endpoint": ...}` alone isn't enough to talk to the
        # emulator: the generated client only switches to a plaintext channel when it
        # sees `PUBSUB_EMULATOR_HOST` set at construction time (see
        # `PublisherClient.__init__`); otherwise it defaults to a TLS channel, which the
        # emulator's plaintext gRPC server can't complete a handshake with. Passing an
        # explicit insecure channel sidesteps that env var dependency entirely.
        # `PublisherClient` rejects `credentials=` alongside an already-constructed
        # `transport=` instance ("provide its credentials directly") - the channel
        # already carries no auth, so pass no credentials here.
        channel = grpc.insecure_channel(f"{host}:{port}")
        client = PublisherClient(transport=PublisherGrpcTransport(channel=channel))
        try:
            client.list_topics(project="projects/mersal-responsive-check", timeout=timeout)
            return True
        finally:
            client.stop()

    try:
        return await run_sync(_check)
    except (OSError, GoogleAPICallError):
        return False


@pytest.fixture()
async def gcp_pubsub_service(
    docker_services: DockerServiceRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await docker_services.start("pubsub-emulator", check=pubsub_emulator_responsive, port=8085)
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", f"{docker_services.docker_ip}:8085")


@pytest.fixture
def project_id() -> str:
    """A fresh project id for each test.

    The emulator namespaces everything under ``projects/{project_id}/...``, so - unlike
    the RabbitMQ tests, which reuse a handful of fixed queue names and must therefore
    delete them on teardown to avoid bleed between tests - giving each test its own
    project id gives perfect isolation for free, with nothing to clean up afterwards.
    """
    return f"mersal-test-{uuid.uuid4().hex}"


@pytest.fixture
def event_topic_prefix() -> str:
    return "mersal-topic-"


@pytest.fixture
async def gcp_pubsub_transport_maker(
    project_id: str,
    event_topic_prefix: str,
) -> AsyncGenerator[TransportMaker, None]:
    """Builds `GCPPubSubTransport` instances against the emulator.

    Named distinctly from `transport_maker` on purpose: `BasicTransportTest` defines its
    own (raising) `transport_maker` fixture directly on the class, and a class-level
    fixture always shadows a same-named one from conftest.py regardless of where it
    sits in the MRO - so each test class below explicitly overrides `transport_maker`
    to return this one, rather than relying on conftest.py alone.
    """
    created_transports: list[GCPPubSubTransport] = []
    started_transports: list[GCPPubSubTransport] = []
    """Every transport that has actually called `_ensure_started`, in the order it
    first did so - which is when its `BlockingPortal` was entered on this fixture's
    task (see the teardown loop below for why that order, not construction order,
    is what matters).
    """

    def maker(**kwargs: Any) -> Transport:
        address = kwargs.pop("input_queue_address")
        config = GCPPubSubTransportConfig(
            project_id=project_id,
            input_queue_name=address,
            event_topic_prefix=event_topic_prefix,
            # `AnyIOPeriodicTask` must be stopped from the task that started it, and
            # under pytest-anyio the test body (which lazily starts the transport) and
            # this fixture's teardown (which closes it) run in different tasks. In
            # production both happen in the lifespan task, so this only matters here.
            **{"consumer_health_check_interval": None, **kwargs},
        )
        transport = GCPPubSubTransport(config=config, logger=NullLogger())
        created_transports.append(transport)

        # `_ensure_started` is lazy and idempotent - tests trigger it in whatever
        # order suits the scenario (e.g. starting a "receiver" before ever touching
        # a "sender" constructed earlier), which need not match construction order.
        # Wrapping it here records the true order transports' portals were entered
        # in, so teardown can close them in exact reverse of that.
        original_ensure_started = transport._ensure_started

        async def _ensure_started_tracked() -> Any:
            if transport not in started_transports:
                started_transports.append(transport)
            return await original_ensure_started()

        transport._ensure_started = _ensure_started_tracked  # type: ignore[method-assign]
        return transport

    yield maker

    # Close transports before the event loop goes away - a still-live consumer's
    # background pump would otherwise be cancelled abruptly at loop teardown. Reverse
    # *start* order matters: `_ensure_started` opens each transport's `BlockingPortal`
    # in this same task, so every portal it opens nests its cancel scope inside
    # whichever one is currently innermost on that task's stack - closing them out of
    # that order would try to exit an outer scope while an inner one is still open,
    # which anyio rejects. Any transport never started has no portal to worry about.
    for transport in reversed(started_transports):
        await transport.close()
    for transport in created_transports:
        if transport not in started_transports:
            await transport.close()


@pytest.fixture
async def gcp_pubsub_subscription_storage_maker(
    project_id: str,
    event_topic_prefix: str,
) -> AsyncGenerator[SubscriptionStorageMaker, None]:
    """Builds `GCPPubSubSubscriptionStorage` instances against the emulator.

    All instances built from one call to this fixture share the same `project_id`, so
    - since GCP Pub/Sub subscriptions are themselves the shared store - they're
    automatically backed by the same underlying state without any extra wiring, which is
    exactly what `BasicSubscriptionStorageTest`'s `centralized_storage_maker` requires.
    """
    created_storages: list[GCPPubSubSubscriptionStorage] = []

    def maker(**kwargs: Any) -> SubscriptionStorage:
        config = GCPPubSubSubscriptionStorageConfig(
            project_id=project_id,
            event_topic_prefix=event_topic_prefix,
            **kwargs,
        )
        storage = GCPPubSubSubscriptionStorage(config=config)
        created_storages.append(storage)
        return storage

    yield maker

    for storage in created_storages:
        await storage.close()
