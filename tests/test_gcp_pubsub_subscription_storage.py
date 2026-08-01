import uuid
from typing import cast

import anyio
import pytest
from google.api_core.exceptions import NotFound
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

from mersal.transport import DefaultTransactionContext
from mersal_gcp_pubsub.subscription_storage import (
    GCPPubSubSubscriptionStorage,
    GCPPubSubSubscriptionStorageConfig,
)
from mersal_gcp_pubsub.transport import GCPPubSubTransport
from mersal_testing.subscription.basic_subscription_storage_tests import (
    BasicSubscriptionStorageTest,
    SubscriptionStorageMaker,
)
from mersal_testing.test_doubles import TransportMessageBuilder
from mersal_testing.testing_utils import is_docker_available
from mersal_testing.transport.basic_transport_tests import TransportMaker

__all__ = ("TestGCPPubSubSubscriptionStorage",)


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.usefixtures("gcp_pubsub_service"),
    pytest.mark.skipif(not is_docker_available(), reason="docker not available on this platform"),
]


class TestGCPPubSubSubscriptionStorage(BasicSubscriptionStorageTest):
    # GCPPubSubSubscriptionStorage has no notion of a topic "owner" to route
    # subscribe/unsubscribe control messages to - every instance can create or delete
    # a subscription directly - so decentralized mode isn't offered.
    supports_decentralized = False

    @pytest.fixture
    def centralized_storage_maker(
        self, gcp_pubsub_subscription_storage_maker: SubscriptionStorageMaker
    ) -> SubscriptionStorageMaker:
        return gcp_pubsub_subscription_storage_maker

    async def test_centralized_storage_is_shared_across_instances(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        centralized_storage_maker: SubscriptionStorageMaker,
        project_id: str,
        event_topic_prefix: str,
    ) -> None:
        """Overrides the base assertion.

        `GCPPubSubSubscriptionStorage.get_subscriber_addresses` returns a single
        synthetic `"{topic}@{event_topic_prefix}"` address rather than the literal
        subscriber addresses (see the class docstring for why), so it can't satisfy the
        base test's literal address-equality check. This verifies the same underlying
        guarantee instead - a subscription created through one instance is visible and
        effective project-wide, not cached per-instance - via an actual publish/pull
        round trip through a subscription that a *second*, independently constructed
        instance then deletes.
        """
        subject1 = centralized_storage_maker()
        subject2 = centralized_storage_maker()
        assert subject1.is_centralized
        assert subject2.is_centralized

        topic = "topic"
        subscriber_address = f"subscriber-{uuid.uuid4()}"

        await subject1.register_subscriber(topic, subscriber_address)

        magic_address = f"{topic}@{event_topic_prefix}"
        assert await subject1.get_subscriber_addresses(topic) == {magic_address}
        assert await subject2.get_subscriber_addresses(topic) == {magic_address}

        publisher = PublisherClient()
        subscriber = SubscriberClient()
        try:
            topic_path = publisher.topic_path(project_id, f"{event_topic_prefix}{topic}")
            subscription_path = subscriber.subscription_path(project_id, f"{subscriber_address}--{topic}")

            publisher.publish(topic_path, b"hello").result(timeout=5)
            response = subscriber.pull(subscription=subscription_path, max_messages=1, timeout=5)
            assert len(response.received_messages) == 1
            assert response.received_messages[0].message.data == b"hello"
            subscriber.acknowledge(subscription=subscription_path, ack_ids=[response.received_messages[0].ack_id])

            # Unregister through the *other* instance, proving the subscription isn't
            # cached per-instance.
            await subject2.unregister_subscriber(topic, subscriber_address)

            publisher.publish(topic_path, b"world").result(timeout=5)
            with pytest.raises(NotFound):
                subscriber.pull(subscription=subscription_path, max_messages=1, timeout=5)
        finally:
            publisher.stop()
            subscriber.close()
            await cast("GCPPubSubSubscriptionStorage", subject1).close()
            await cast("GCPPubSubSubscriptionStorage", subject2).close()

    async def test_register_and_unregister_are_idempotent(
        self,
        centralized_storage_maker: SubscriptionStorageMaker,
    ) -> None:
        """Registering/unregistering twice mirrors plain `create_subscription`/
        `delete_subscription` semantics - both errors (`AlreadyExists`, `NotFound`) are
        suppressed - useful since apps may resubscribe on every restart.
        """
        subject = centralized_storage_maker()
        topic = "topic"
        subscriber_address = f"subscriber-{uuid.uuid4()}"

        try:
            await subject.register_subscriber(topic, subscriber_address)
            await subject.register_subscriber(topic, subscriber_address)

            await subject.unregister_subscriber(topic, subscriber_address)
            await subject.unregister_subscriber(topic, subscriber_address)
        finally:
            await cast("GCPPubSubSubscriptionStorage", subject).close()

    async def test_register_subscriber_starts_live_consumption_for_the_owning_transport(
        self,
        gcp_pubsub_transport_maker: TransportMaker,
        project_id: str,
        event_topic_prefix: str,
    ) -> None:
        """Unlike RabbitMQ - where a new binding on an already-running queue needs no
        new consumer - registering a subscription for an address genuinely needs a new
        live pull consumer if that address is the transport this storage is paired
        with (see `GCPPubSubTransport.start_consuming_topic`). This exercises that
        coupling end-to-end: a topic subscribed to *after* the transport has already
        started is received without any restart, and unsubscribing stops consuming it.
        """
        receiver = cast("GCPPubSubTransport", gcp_pubsub_transport_maker(input_queue_address="dyn-sub-receiver"))
        publisher = cast("GCPPubSubTransport", gcp_pubsub_transport_maker(input_queue_address="dyn-sub-publisher"))
        await receiver()

        storage_config = GCPPubSubSubscriptionStorageConfig(
            project_id=project_id, event_topic_prefix=event_topic_prefix
        )
        storage = GCPPubSubSubscriptionStorage(config=storage_config, transport=receiver)

        topic = "dynamically.subscribed.topic"
        await storage.register_subscriber(topic, receiver.address)
        assert topic in receiver._state.topic_consumers  # type: ignore[union-attr]

        magic_address = next(iter(await storage.get_subscriber_addresses(topic)))
        message = TransportMessageBuilder.build()

        async with DefaultTransactionContext() as send_context:
            await publisher.send(magic_address, message, send_context)
            send_context.set_result(commit=True, ack=True)
            await send_context.complete()

        async with DefaultTransactionContext() as receive_context:
            with anyio.fail_after(5.0):
                received = await receiver.receive(receive_context)
            assert received is not None
            assert str(received.headers.message_id) == str(message.headers.message_id)
            receive_context.set_result(commit=True, ack=True)
            await receive_context.complete()

        await storage.unregister_subscriber(topic, receiver.address)
        assert topic not in receiver._state.topic_consumers  # type: ignore[union-attr]

        await storage.close()
