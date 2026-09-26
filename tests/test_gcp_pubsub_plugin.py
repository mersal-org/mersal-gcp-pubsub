import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any

import anyio
import pytest

from mersal.activation import BuiltinHandlerActivator
from mersal.core.app import Mersal
from mersal.gcp_pubsub.plugin import GCPPubSubPluginConfig
from mersal.persistence.in_memory import InMemoryTimeoutManager
from mersal.testing.core.testing_utils import is_docker_available
from mersal.timeouts import TimeoutsConfig

__all__ = ("TestGCPPubSubPlugin",)


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.usefixtures("gcp_pubsub_service"),
    pytest.mark.skipif(not is_docker_available(), reason="docker not available on this platform"),
]


@dataclass
class Greeting:
    text: str


class _JsonSerializer:
    """Bytes-on-the-wire serializer for this test only.

    The transport/subscription-storage tests build `TransportMessage`s directly
    (`TransportMessageBuilder`), bypassing app-level serialization entirely. This test
    goes through a real `Mersal` app instead, so it needs a `Serializer` that actually
    turns a message into the bytes Pub/Sub requires.
    """

    def __init__(self, types: set[type]) -> None:
        self._types = {t.__name__: t for t in types}

    def serialize(self, obj: Any) -> bytes:
        return json.dumps({"type": type(obj).__name__, "data": asdict(obj)}).encode()

    def deserialize(self, data: bytes) -> Any:
        payload = json.loads(data)
        return self._types[payload["type"]](**payload["data"])


class TestGCPPubSubPlugin:
    """End-to-end check that `GCPPubSubPluginConfig` (the way `docs/usage.rst` tells
    users to configure this package) wires a working transport and subscription
    storage into a real `Mersal` app, on top of the transport/subscription-storage
    tests exercising those pieces directly.
    """

    def _make_app(
        self, project_id: str, event_topic_prefix: str, queue_name: str, **app_kwargs: Any
    ) -> tuple[Mersal, list[Greeting], anyio.Event]:
        received: list[Greeting] = []
        done = anyio.Event()

        activator = BuiltinHandlerActivator()

        def handler_factory(message_context: Any, app: Mersal) -> Callable[[Greeting], Awaitable[None]]:
            async def handler(message: Greeting) -> None:
                received.append(message)
                done.set()

            return handler

        activator.register(Greeting, handler_factory)

        plugin_config = GCPPubSubPluginConfig(
            project_id=project_id,
            input_queue_name=queue_name,
            event_topic_prefix=event_topic_prefix,
            consumer_health_check_interval=None,
        )

        app = Mersal(
            "plugin-test-app",
            activator,
            plugins=[plugin_config.plugin()],
            serializer=_JsonSerializer(types={Greeting}),
            **app_kwargs,
        )
        return app, received, done

    async def test_send_local_is_delivered_to_the_registered_handler(
        self,
        project_id: str,
        event_topic_prefix: str,
    ) -> None:
        queue_name = f"plugin-test-{uuid.uuid4()}"
        app, received, done = self._make_app(project_id, event_topic_prefix, queue_name)

        try:
            await app.start()
            await app.send_local(Greeting(text="hello"))

            with anyio.fail_after(5.0):
                await done.wait()

            assert received == [Greeting(text="hello")]
        finally:
            await app.stop()

    async def test_defer_local_is_delivered_through_the_timeout_manager(
        self,
        project_id: str,
        event_topic_prefix: str,
    ) -> None:
        """Pub/Sub can't delay delivery itself, so deferring goes through a timeout
        manager: the message is sent to this app's own address, stored until due, and
        then sent to its recipient."""
        queue_name = f"plugin-test-{uuid.uuid4()}"
        timeout_manager = InMemoryTimeoutManager()
        app, received, done = self._make_app(
            project_id,
            event_topic_prefix,
            queue_name,
            timeouts=TimeoutsConfig(storage=timeout_manager, poll_interval=0.1),
        )

        try:
            await app.start()
            await app.defer_local(timedelta(seconds=1), Greeting(text="later"))

            with anyio.fail_after(5.0):
                while not len(timeout_manager):
                    await anyio.sleep(0.05)
            assert not done.is_set()
            with anyio.fail_after(10.0):
                await done.wait()

            assert received == [Greeting(text="later")]
        finally:
            await app.stop()

    async def test_publish_after_subscribe_is_delivered_to_the_registered_handler(
        self,
        project_id: str,
        event_topic_prefix: str,
    ) -> None:
        """Verifies the plugin actually threads the transport into the subscription
        storage (see `GCPPubSubPlugin.register_subscription_storage`): subscribing
        after the app - and therefore its transport - has already started only works
        if `register_subscriber` can start a live consumer on the fly.
        """
        queue_name = f"plugin-test-{uuid.uuid4()}"
        app, received, done = self._make_app(project_id, event_topic_prefix, queue_name)

        try:
            await app.start()
            await app.subscribe(Greeting)
            await app.publish(Greeting(text="hello"))

            with anyio.fail_after(5.0):
                await done.wait()

            assert received == [Greeting(text="hello")]
        finally:
            await app.stop()

    async def test_send_only_app_can_send_to_a_receiving_app(
        self,
        project_id: str,
        event_topic_prefix: str,
    ) -> None:
        """A send-only app (`Mersal(..., send_only=True)`) still gets a working
        transport wired up by `GCPPubSubPlugin`, proving `configurator.send_only`
        reaches `GCPPubSubTransportConfig`.
        """
        receiver_queue_name = f"plugin-test-receiver-{uuid.uuid4()}"
        sender_queue_name = f"plugin-test-sender-{uuid.uuid4()}"
        receiver, received, done = self._make_app(project_id, event_topic_prefix, receiver_queue_name)

        sender_plugin_config = GCPPubSubPluginConfig(
            project_id=project_id,
            input_queue_name=sender_queue_name,
            event_topic_prefix=event_topic_prefix,
            consumer_health_check_interval=None,
        )
        sender = Mersal(
            "plugin-test-sender",
            BuiltinHandlerActivator(),
            plugins=[sender_plugin_config.plugin()],
            serializer=_JsonSerializer(types={Greeting}),
            send_only=True,
        )

        try:
            await receiver.start()
            await sender.start()

            assert sender.worker is None

            await sender.send(Greeting(text="hello"), addresses={receiver_queue_name})

            with anyio.fail_after(5.0):
                await done.wait()

            assert received == [Greeting(text="hello")]
        finally:
            await sender.stop()
            await receiver.stop()
