import uuid
from typing import cast

import anyio
import pytest
from google.api_core.exceptions import NotFound

from mersal.logging import NullLogger
from mersal.transport import DefaultTransactionContext
from mersal.types.callable_types import AsyncAnyCallable
from mersal_gcp_pubsub.transport import GCPPubSubTransport, GCPPubSubTransportConfig
from mersal_testing.test_doubles import TransportMessageBuilder
from mersal_testing.testing_utils import is_docker_available
from mersal_testing.transport.basic_transport_tests import BasicTransportTest, TransportMaker

__all__ = (
    "TestGCPPubSubTransport",
    "TestGCPPubSubTransportContract",
    "TestGCPPubSubTransportSpecificBehaviour",
)


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.usefixtures("gcp_pubsub_service"),
    pytest.mark.skipif(not is_docker_available(), reason="docker not available on this platform"),
]


class TestGCPPubSubTransport:
    async def test_create_queue(self, project_id: str, event_topic_prefix: str) -> None:
        queue_name = f"queue-{uuid.uuid4()}"
        config = GCPPubSubTransportConfig(
            project_id=project_id,
            input_queue_name=queue_name,
            event_topic_prefix=event_topic_prefix,
            consumer_health_check_interval=None,
        )
        transport = GCPPubSubTransport(config=config, logger=NullLogger())
        await transport()

        state = transport._state
        assert state is not None
        assert state.own_consumer is not None
        subscription = state.subscriber.get_subscription(subscription=state.own_consumer.subscription_path)
        assert subscription.name == state.own_consumer.subscription_path

        await transport.close()


class TestGCPPubSubTransportContract(BasicTransportTest):
    """Runs mersal_testing's generic transport contract against `GCPPubSubTransport`."""

    @pytest.fixture
    def transport_maker(self, gcp_pubsub_transport_maker: TransportMaker) -> TransportMaker:
        return gcp_pubsub_transport_maker


class TestGCPPubSubTransportSpecificBehaviour:
    """GCP-specific behaviour that the generic transport contract doesn't (and
    shouldn't) know about: native pub/sub fan-out via per-subscriber subscriptions on a
    shared topic, string-only header round-tripping, and self-healing after a pull
    consumer's underlying stream dies.
    """

    @pytest.fixture
    def transport_maker(self, gcp_pubsub_transport_maker: TransportMaker) -> TransportMaker:
        return gcp_pubsub_transport_maker

    #: `receive()` has no built-in timeout - it blocks until a message arrives - so
    #: every receive expecting a message runs under this external deadline, turning a
    #: delivery regression into a fast `TimeoutError` instead of a hung test.
    receive_deadline: float = 5.0

    async def assert_with_context(
        self,
        assertions_call: AsyncAnyCallable,
        commit: bool = True,
        ack: bool = True,
    ) -> None:
        async with DefaultTransactionContext() as context:
            await assertions_call(context)
            context.set_result(commit=commit, ack=ack)
            await context.complete()

    async def test_send_fails_loud_when_topology_management_is_disabled_and_destination_missing(
        self, transport_maker: TransportMaker
    ) -> None:
        """Unlike RabbitMQ's `mandatory` flag, GCP Pub/Sub always fails a publish to a
        topic that doesn't exist - there's no separate "fire and forget" mode to
        disable. So with topology management off and nobody having ever provisioned the
        destination, the send fails loud rather than silently vanishing.
        """
        sender = cast(
            "GCPPubSubTransport",
            transport_maker(input_queue_address="unmanaged-sender", should_declare_topics=False),
        )
        message = TransportMessageBuilder.build()

        async def _send(context: DefaultTransactionContext) -> None:
            await sender.send("this-destination-was-never-created", message, context)

        with pytest.raises(NotFound):
            await self.assert_with_context(_send)

    async def test_custom_headers_round_trip_as_strings(self, transport_maker: TransportMaker) -> None:
        """Pub/Sub message attributes are a plain `map<string, string>`, so a header
        sent as a non-`str` value (an `int` here) comes back as a `str` - unlike a
        transport with a richer header encoding, where it would round-trip unchanged.
        """
        sender = transport_maker(input_queue_address="header-sender")
        receiver = transport_maker(input_queue_address="header-receiver")
        message = TransportMessageBuilder.build()
        message.headers["x-custom-header"] = 10

        # Starts the receiver so its topic/subscription exist before the sender addresses it.
        await receiver()

        async def _send(context: DefaultTransactionContext) -> None:
            await sender.send("header-receiver", message, context)

        await self.assert_with_context(_send)

        async def _receive(context: DefaultTransactionContext) -> None:
            with anyio.fail_after(self.receive_deadline):
                received = await receiver.receive(context)
            assert received is not None
            assert received.headers["x-custom-header"] == "10"
            assert str(received.headers.message_id) == str(message.headers.message_id)

        await self.assert_with_context(_receive)

    async def test_publish_to_topic_fans_out_to_every_subscribed_consumer(
        self, transport_maker: TransportMaker, event_topic_prefix: str
    ) -> None:
        """Exercises the "magic address" pub/sub path directly at the transport level:
        an address of the form `topic@marker` is published once to the shared event
        topic, and every subscription on it (simulating what
        `GCPPubSubSubscriptionStorage.register_subscriber` does) receives its own
        copy - broker-native fan-out, not N point-to-point sends.
        """
        publisher = cast("GCPPubSubTransport", transport_maker(input_queue_address="fanout-publisher"))
        subscriber1 = cast("GCPPubSubTransport", transport_maker(input_queue_address="fanout-sub-1"))
        subscriber2 = cast("GCPPubSubTransport", transport_maker(input_queue_address="fanout-sub-2"))

        await subscriber1()
        await subscriber2()

        topic = "some.event.topic"
        # Directly declares each subscriber's own subscription on the event topic,
        # bypassing `GCPPubSubSubscriptionStorage` (covered separately) - this test is
        # only about the transport's own send/consume wiring for the magic address.
        for subscriber in (subscriber1, subscriber2):
            state = await subscriber._ensure_started()
            topic_path = await subscriber._ensure_topic(state.publisher, subscriber._event_topic_id(topic))
            await subscriber._ensure_subscription(
                state.subscriber, subscriber._pubsub_subscription_id(topic), topic_path
            )
            await subscriber.start_consuming_topic(topic)

        message = TransportMessageBuilder.build()
        magic_address = f"{topic}@{event_topic_prefix}"

        async def _publish(context: DefaultTransactionContext) -> None:
            await publisher.send(magic_address, message, context)

        await self.assert_with_context(_publish)

        async def _assert_delivered(context: DefaultTransactionContext) -> None:
            with anyio.fail_after(self.receive_deadline):
                received1 = await subscriber1.receive(context)
                received2 = await subscriber2.receive(context)
            assert received1 is not None
            assert received2 is not None
            assert str(received1.headers.message_id) == str(message.headers.message_id)
            assert str(received2.headers.message_id) == str(message.headers.message_id)

        await self.assert_with_context(_assert_delivered)

    async def test_publish_to_topic_with_no_subscribers_does_not_raise(
        self, transport_maker: TransportMaker, event_topic_prefix: str
    ) -> None:
        """Unlike a point-to-point send to a destination nobody provisioned, publishing
        an event nobody has subscribed to yet is normal, not an error - `send` ensures
        the event topic exists on the fly (see `_resolve_publish_topic_path`).
        """
        publisher = transport_maker(input_queue_address="fanout-publisher-lonely")
        message = TransportMessageBuilder.build()
        magic_address = f"nobody.listens.to.this@{event_topic_prefix}"

        async def _publish(context: DefaultTransactionContext) -> None:
            await publisher.send(magic_address, message, context)

        await self.assert_with_context(_publish)

    async def test_receive_self_heals_after_consumer_dies(self, transport_maker: TransportMaker) -> None:
        """If the underlying pull consumer dies (e.g. its stream was cancelled), the
        periodic health check transparently reinitializes it rather than leaving the
        transport unable to receive anything ever again.
        """
        sender = transport_maker(input_queue_address="healer-sender")
        receiver = cast("GCPPubSubTransport", transport_maker(input_queue_address="healer"))

        await receiver()
        state = receiver._state
        assert state is not None
        assert state.own_consumer is not None

        state.own_consumer.future.cancel()
        with anyio.fail_after(self.receive_deadline):
            while not state.own_consumer.future.done():
                await anyio.sleep(0.05)

        await receiver._check_consumers_health()
        assert state.own_consumer.future.done() is False

        message = TransportMessageBuilder.build()

        async def _send(context: DefaultTransactionContext) -> None:
            await sender.send("healer", message, context)

        await self.assert_with_context(_send)

        async def _receive(context: DefaultTransactionContext) -> None:
            with anyio.fail_after(self.receive_deadline):
                received = await receiver.receive(context)
            assert received is not None
            assert str(received.headers.message_id) == str(message.headers.message_id)

        await self.assert_with_context(_receive)

    async def test_consumer_health_check_disabled_when_interval_is_none(self, transport_maker: TransportMaker) -> None:
        transport = cast(
            "GCPPubSubTransport",
            transport_maker(input_queue_address="health-check-disabled", consumer_health_check_interval=None),
        )
        assert transport._health_check_task is None
