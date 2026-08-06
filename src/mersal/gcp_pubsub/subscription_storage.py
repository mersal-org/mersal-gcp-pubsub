from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

import anyio
from anyio.to_thread import run_sync
from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

from mersal.subscription import SubscriptionStorage

if TYPE_CHECKING:
    from mersal.gcp_pubsub.transport import GCPPubSubTransport

__all__ = (
    "GCPPubSubSubscriptionStorage",
    "GCPPubSubSubscriptionStorageConfig",
)


@dataclass
class GCPPubSubSubscriptionStorageConfig:
    project_id: str
    event_topic_prefix: str = "mersal-topic-"
    """Must match the `event_topic_prefix` of the `GCPPubSubTransport` instances sharing
    this project - subscribing and publishing only agree on where events land if both
    sides derive the same topic id from a given Mersal topic name.
    """
    should_declare_event_topics: bool = True
    ack_deadline_seconds: int = 60
    """Ack deadline used when declaring a subscriber's subscription. Should match the
    `ack_deadline_seconds` the subscriber's own `GCPPubSubTransport` uses for its own
    subscription, for consistent redelivery behaviour across everything that app
    consumes.
    """


class GCPPubSubSubscriptionStorage(SubscriptionStorage):
    """A `SubscriptionStorage` that stores subscriptions as native GCP Pub/Sub subscriptions.

    Unlike the RabbitMQ transport - where subscribing is just a new binding on a queue
    that's already being consumed - GCP Pub/Sub requires a dedicated `Subscription`
    (and therefore a dedicated pull consumer) per subscribed topic. This storage
    therefore needs a reference to the subscribing app's own `GCPPubSubTransport`, to
    start/stop that consumer whenever *that* transport's own address registers or
    unregisters itself. `transport` is optional only so a storage instance can be built
    without a live transport at hand (e.g. tooling that only needs to inspect or manage
    subscriptions for other apps); a storage used to (un)register the address of the
    app it's actually paired with must be given one, or that app will never actually
    receive what it subscribes to.
    """

    def __init__(
        self,
        config: GCPPubSubSubscriptionStorageConfig,
        transport: GCPPubSubTransport | None = None,
    ) -> None:
        self._config = config
        self._transport = transport
        self._publisher: PublisherClient | None = None
        self._subscriber: SubscriberClient | None = None
        self._connect_lock = anyio.Lock()

    @property
    def is_centralized(self) -> bool:
        return True

    async def connect(self) -> None:
        """Eagerly create the underlying clients.

        Optional: every public method also connects lazily on first use, so calling
        this isn't required, but doing so up front (e.g. from an app's startup hook)
        surfaces connection failures before the first `register_subscriber` call.
        """
        await self._ensure_connected()

    async def close(self) -> None:
        """Close the underlying clients.

        The storage can be used again afterwards; the next use reconnects.
        """
        publisher, subscriber = self._publisher, self._subscriber
        self._publisher = None
        self._subscriber = None
        if subscriber is not None:
            with suppress(Exception):
                await run_sync(subscriber.close)
        if publisher is not None:
            with suppress(Exception):
                await run_sync(publisher.stop)

    async def register_subscriber(self, topic: str, subscriber_address: str) -> None:
        publisher, subscriber = await self._ensure_connected()

        topic_path = publisher.topic_path(self._config.project_id, self._event_topic_id(topic))
        if self._config.should_declare_event_topics:
            with suppress(AlreadyExists):
                await run_sync(partial(publisher.create_topic, name=topic_path))

        subscription_path = subscriber.subscription_path(
            self._config.project_id, self._subscription_id(topic, subscriber_address)
        )
        with suppress(AlreadyExists):
            await run_sync(
                partial(
                    subscriber.create_subscription,
                    name=subscription_path,
                    topic=topic_path,
                    ack_deadline_seconds=self._config.ack_deadline_seconds,
                )
            )

        if self._transport is not None and subscriber_address == self._transport.address:
            await self._transport.start_consuming_topic(topic)

    async def unregister_subscriber(self, topic: str, subscriber_address: str) -> None:
        _publisher, subscriber = await self._ensure_connected()

        if self._transport is not None and subscriber_address == self._transport.address:
            await self._transport.stop_consuming_topic(topic)

        subscription_path = subscriber.subscription_path(
            self._config.project_id, self._subscription_id(topic, subscriber_address)
        )
        with suppress(NotFound):
            await run_sync(partial(subscriber.delete_subscription, subscription=subscription_path))

    async def get_subscriber_addresses(self, topic: str) -> set[str]:
        return {f"{topic}@{self._config.event_topic_prefix}"}

    def _event_topic_id(self, topic: str) -> str:
        return f"{self._config.event_topic_prefix}{topic}"

    def _subscription_id(self, topic: str, subscriber_address: str) -> str:
        """Subscription id for `subscriber_address`'s copy of `topic`'s fan-out.

        Must use the same ``f"{subscriber_address}--{topic}"`` convention as
        `GCPPubSubTransport._pubsub_subscription_id`, since that's what actually starts
        consuming the subscription this creates.
        """
        return f"{subscriber_address}--{topic}"

    async def _ensure_connected(self) -> tuple[PublisherClient, SubscriberClient]:
        if self._publisher is not None and self._subscriber is not None:
            return self._publisher, self._subscriber
        async with self._connect_lock:
            if self._publisher is not None and self._subscriber is not None:
                # Another task finished connecting while we waited on the lock.
                return self._publisher, self._subscriber

            publisher = PublisherClient()
            try:
                subscriber = SubscriberClient()
            except BaseException:
                publisher.stop()
                raise

            self._publisher = publisher
            self._subscriber = subscriber
            return publisher, subscriber
