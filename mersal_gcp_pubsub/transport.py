from __future__ import annotations

import math
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

import anyio
from anyio.from_thread import BlockingPortal
from anyio.to_thread import run_sync
from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl

from mersal.logging import Logger, NullLogger
from mersal.messages import TransportMessage
from mersal.messages.message_headers import MessageHeaders
from mersal.threading import AnyIOPeriodicTaskFactory, PeriodicAsyncTask, PeriodicAsyncTaskFactory
from mersal.transport.base_transport import BaseTransport
from mersal.utils import AsyncRetrier

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
    from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
    from google.cloud.pubsub_v1.subscriber.message import Message

    from mersal.transport import TransactionContext
    from mersal.transport.outgoing_message import OutgoingMessage

__all__ = (
    "GCPPubSubTransport",
    "GCPPubSubTransportConfig",
)


_RETRY_DELAYS = [0.5, 2.0]

_RESERVED_PUBLISH_ATTRIBUTE_KEYS = frozenset({"ordering_key", "retry", "timeout"})


@dataclass
class GCPPubSubTransportConfig:
    project_id: str
    input_queue_name: str
    send_only: bool = False
    """Set by `GCPPubSubPlugin` from the owning app's `send_only` configuration - a
    send-only app never receives, so this transport skips creating its own topic,
    subscription, and pull consumer entirely.
    """
    should_declare_topics: bool = True
    should_declare_subscriptions: bool = True
    direct_topic_prefix: str = "mersal-direct-"
    """Prefix used to derive the GCP topic id backing a plain (point-to-point) address."""
    event_topic_prefix: str = "mersal-topic-"
    """Prefix used to derive the GCP topic id backing a pub/sub topic.

    Must match the `event_topic_prefix` of any
    `mersal_gcp_pubsub.subscription_storage.GCPPubSubSubscriptionStorage` sharing this
    project - subscribing and publishing only agree on where events land if both sides
    derive the same topic id from a given Mersal topic name.
    """
    ack_deadline_seconds: int = 60
    """The subscription's ack deadline, in seconds: how long Pub/Sub waits for an ack
    before redelivering a message.

    This is a dead-puller safety net, not a processing-time budget - while this
    transport is alive it keeps a message's deadline extended for as long as
    `max_lease_duration_seconds` allows, regardless of what `ack_deadline_seconds` is
    set to. It only ends up mattering when there's no one left to extend the deadline:
    a puller that crashes without nacking leaves its in-flight messages stuck for up to
    this long before Pub/Sub redelivers them.
    """
    max_outstanding_messages: int = 50
    """Maximum number of received - but not yet acked/nacked - messages a single pull
    consumer will hold locally before pausing delivery. Sized a few times larger than
    the app's expected processing concurrency (see `max_parallelism` on the worker) keeps
    the pipeline fed without letting one slow consumer buffer unbounded messages in memory.
    """
    max_lease_duration_seconds: float = 3600.0
    """The processing-time budget, in seconds: how long since receiving a message the
    client library will keep auto-extending its ack deadline (via modack) on behalf of
    this transport before giving up and letting it lapse for redelivery.

    Size this to the longest a message may legitimately take to process; the
    per-extension deadline is auto-tuned from recent ack latency, so this is the only
    knob that bounds the total.
    """
    consumer_health_check_interval: float | None = 60.0
    """How often, in seconds, to check whether any live pull consumer's underlying
    stream has died (e.g. a fatal, non-retryable error - transient ones are already
    retried internally by the client library) and restart it if so.

    Set to `None` to disable it entirely.
    """


@dataclass
class _Consumer:
    """A live pull consumer for a single subscription."""

    subscription_path: str
    future: StreamingPullFuture


@dataclass
class _StartedState:
    """Everything that only exists once the transport has connected."""

    publisher: PublisherClient
    subscriber: SubscriberClient
    own_consumer: _Consumer | None
    """None for a send-only transport, which never creates its own topic,
    subscription, or pull consumer."""
    topic_consumers: dict[str, _Consumer]
    """Live consumers for pub/sub topics this app's own address is subscribed to,
    keyed by Mersal topic name. Populated dynamically by `start_consuming_topic` -
    typically called by `GCPPubSubSubscriptionStorage.register_subscriber` - rather
    than all being known upfront at start time.
    """
    portal: BlockingPortal
    """Lets the foreign threads `SubscriberClient.subscribe` delivers on safely hand
    messages back into this transport's event loop. See `_start_streaming_pull`.
    """
    send_stream: MemoryObjectSendStream[Message]
    receive_stream: MemoryObjectReceiveStream[Message]


class GCPPubSubTransport(BaseTransport):
    """A GCP Pub/Sub transport using pull delivery (streaming pull) for consumption.

    Topology:
        Every Mersal address - both an app's own point-to-point address and a pub/sub
        topic name - becomes its own GCP topic, since Pub/Sub has no concept of routing
        keys or exchanges: a plain address ``"billing"`` becomes topic
        ``f"{direct_topic_prefix}billing"``, while an event topic ``"order.created"``
        becomes ``f"{event_topic_prefix}order.created"``. Each app's own input queue is
        backed by a single dedicated subscription (named after its address) bound to
        its own direct topic. Pub/sub fan-out is native: every subscriber gets its own
        subscription on the *same* event topic (see
        `mersal_gcp_pubsub.subscription_storage.GCPPubSubSubscriptionStorage`), so
        publishing once reaches everyone subscribed.

    Push-pull bridge:
        `SubscriberClient.subscribe` delivers messages by invoking a callback on a
        thread pool it manages internally - a push model Every subscription's
        callback is a foreign thread as far as the event loop is concerned, so it hands
        each message to `receive` via an `anyio.from_thread.BlockingPortal`: the
        callback does `portal.call(send_stream.send_nowait, message)` on a shared
        anyio memory object stream, and `receive` is just
        `await receive_stream.receive()`. One portal and one stream pair serve every
        subscription this transport consumes - this app's own, plus one per pub/sub
        topic it's subscribed to.

    Startup:
        Connecting is lazy: the first call to `send`, `receive`, or `create_queue`
        creates the underlying clients and this app's own topic/subscription/consumer
        if they don't exist yet. `__call__` (registered as an on-startup lifespan hook
        by `GCPPubSubPlugin`) simply triggers this eagerly. After `close`, the next use
        starts the transport again from scratch.

    Self-healing:
        See `GCPPubSubTransportConfig.consumer_health_check_interval`.

    Send-only apps:
        See `GCPPubSubTransportConfig.send_only`: this app's own topic, subscription,
        and pull consumer are only ever created when it isn't send-only, since a
        send-only app never receives.
    """

    def __init__(
        self,
        config: GCPPubSubTransportConfig,
        periodic_task_factory: PeriodicAsyncTaskFactory | None = None,
        logger: Logger | None = None,
    ):
        super().__init__(config.input_queue_name)

        self._logger = logger or NullLogger()

        self._project_id = config.project_id
        self._send_only = config.send_only
        self._should_declare_topics = config.should_declare_topics
        self._should_declare_subscriptions = config.should_declare_subscriptions
        self._direct_topic_prefix = config.direct_topic_prefix
        self._event_topic_prefix = config.event_topic_prefix
        self._ack_deadline_seconds = config.ack_deadline_seconds
        self._flow_control = FlowControl(
            max_messages=config.max_outstanding_messages,
            max_lease_duration=config.max_lease_duration_seconds,
        )

        periodic_task_factory = periodic_task_factory or AnyIOPeriodicTaskFactory(logger=self._logger)
        self._health_check_task: PeriodicAsyncTask | None = (
            periodic_task_factory(
                f"GCPPubSub-HealthCheck-{config.input_queue_name}",
                self._check_consumers_health,
                config.consumer_health_check_interval,
            )
            if config.consumer_health_check_interval is not None and not self._send_only
            else None
        )

        self._retrier = AsyncRetrier(_RETRY_DELAYS)

        self._state: _StartedState | None = None
        self._start_lock = anyio.Lock()
        self._topic_consumer_lock = anyio.Lock()
        self._ensured_topic_ids: set[str] = set()
        """Topic ids already confirmed to exist, so `_ensure_topic` issues the
        `create_topic` admin RPC (expecting `AlreadyExists`) once per topic for this
        transport's lifetime rather than on every publish.
        """

    async def __call__(self) -> None:
        await self._ensure_started()

    async def close(self) -> None:
        """Gracefully tear down every live consumer and the underlying clients.

        Wired up as an on-shutdown lifespan hook by `GCPPubSubPlugin`. The transport can
        be used again afterwards; the next use starts it from scratch.
        """
        if self._health_check_task is not None:
            await self._health_check_task.stop()
        state = self._state
        self._state = None
        if state is None:
            return

        all_consumers = (state.own_consumer, *state.topic_consumers.values())
        for consumer in all_consumers:
            if consumer is not None:
                await self._stop_consumer(consumer)

        # Every consumer is stopped, so nothing can hand off a new message anymore;
        # shut the portal down before draining so no in-flight callback is racing
        # `receive_nowait` below.
        await state.portal.__aexit__(None, None, None)

        # Anything still sitting in the stream at this point would otherwise stay
        # invisible to every other consumer until `ack_deadline_seconds` elapses. Nack
        # each for prompt redelivery - the Pub/Sub equivalent of AMQP requeue-on-
        # channel-close.
        while True:
            try:
                message = state.receive_stream.receive_nowait()
            except (anyio.WouldBlock, anyio.EndOfStream):
                break
            message.nack()

        with suppress(Exception):
            state.publisher.stop()
        with suppress(Exception):
            await run_sync(state.subscriber.close)

    async def create_queue(self, address: str) -> None:
        """Ensure `address` can receive point-to-point sends even before its owning app
        has ever started.

        Pub/Sub discards messages published to a topic with no subscriptions - unlike
        RabbitMQ, where declaring a queue is what gives it retention, here the
        subscription *is* the retention. So this must create both the direct topic and
        `address`'s own subscription on it (the same subscription that app would create
        for itself in `_ensure_started`), not just the topic - otherwise a send to a
        not-yet-started peer is a silent, successful-looking message loss.
        """
        state = await self._ensure_started()
        if address != self.address:
            topic_path = await self._ensure_topic(state.publisher, self._direct_topic_id(address))
            await self._ensure_subscription(state.subscriber, address, topic_path)

    async def send_outgoing_messages(
        self,
        outgoing_message: list[OutgoingMessage],
        transaction_context: TransactionContext,
    ) -> None:
        """Publish every message in the batch concurrently rather than one at a time.

        The publisher client batches outgoing messages internally (a short linger
        window), but only across `publish()` calls that are in flight at the same time.
        Awaiting each message's result before firing the next serializes what should be
        one batched RPC into N sequential ones. Running each send as its own task means
        every `publish()` call in the batch fires before any of them blocks on
        `future.result()` (anyio schedules each task up to its first real suspension
        point), letting the client's batching actually kick in.
        """
        state = await self._ensure_started()

        async def _send(message: OutgoingMessage) -> None:
            topic_path = await self._resolve_publish_topic_path(state, message.destination_address)
            await self._retrier.run(partial(self._publish, state, topic_path, message.transport_message))

        async with anyio.create_task_group() as task_group:
            for message in outgoing_message:
                _ = task_group.start_soon(_send, message)

    async def receive(self, transaction_context: TransactionContext) -> TransportMessage | None:
        """Wait for the next message; blocks until one arrives.

        There is deliberately no built-in timeout: the worker stops a blocked receive
        by cancelling it, and any caller needing a deadline can impose one externally
        (e.g. `anyio.move_on_after`) - the wait is cancellation-safe.
        """
        state = await self._ensure_started()
        message = await state.receive_stream.receive()

        # `ack()`/`nack()` are non-blocking - they just enqueue onto the streaming
        # pull manager's own dispatcher thread - so unlike `_publish`, there's no
        # actual I/O here to hop onto a thread for or retry.
        async def on_ack(_: TransactionContext) -> None:
            message.ack()

        async def on_nack(_: TransactionContext) -> None:
            message.nack()

        transaction_context.on_ack(on_ack)
        transaction_context.on_nack(on_nack)

        return self._to_transport_message(message)

    async def start_consuming_topic(self, topic: str) -> None:
        """Start (idempotently) a live pull consumer for this app's own subscription on
        `topic`'s fan-out.

        Called by `GCPPubSubSubscriptionStorage.register_subscriber` whenever *this*
        transport's own address subscribes to a topic. Unlike RabbitMQ - where a new
        binding on an already-running queue needs no new consumer - GCP requires a
        dedicated `Subscription` (and therefore a dedicated pull consumer) per
        subscribed topic, so subscribing to a new topic at runtime genuinely means
        starting a new consumer here, not just a broker-side registration.
        """
        state = await self._ensure_started()
        async with self._topic_consumer_lock:
            if topic in state.topic_consumers:
                return
            subscription_path = state.subscriber.subscription_path(
                self._project_id, self._pubsub_subscription_id(topic)
            )
            future = self._start_streaming_pull(state.subscriber, subscription_path, state.portal, state.send_stream)
            state.topic_consumers[topic] = _Consumer(subscription_path=subscription_path, future=future)

    async def stop_consuming_topic(self, topic: str) -> None:
        """Stop and forget the live pull consumer for `topic`, if one is running."""
        state = await self._ensure_started()
        async with self._topic_consumer_lock:
            consumer = state.topic_consumers.pop(topic, None)
        if consumer is not None:
            await self._stop_consumer(consumer)

    def _direct_topic_id(self, address: str) -> str:
        return f"{self._direct_topic_prefix}{address}"

    def _event_topic_id(self, topic: str) -> str:
        return f"{self._event_topic_prefix}{topic}"

    def _pubsub_subscription_id(self, topic: str) -> str:
        """Subscription id for this app's own copy of `topic`'s fan-out.

        Must use the same ``f"{subscriber_address}--{topic}"`` convention as
        `GCPPubSubSubscriptionStorage.register_subscriber`, since that's what actually
        creates the subscription on the broker - this only starts consuming it.
        """
        return f"{self.address}--{topic}"

    async def _ensure_started(self) -> _StartedState:
        if self._state is not None:
            return self._state
        async with self._start_lock:
            publisher = PublisherClient()
            try:
                subscriber = SubscriberClient()
            except BaseException:
                publisher.stop()
                raise

            portal = BlockingPortal()
            try:
                await portal.__aenter__()
            except BaseException:
                with anyio.CancelScope(shield=True):
                    await run_sync(subscriber.close)
                publisher.stop()
                raise

            try:
                send_stream: MemoryObjectSendStream[Message]
                receive_stream: MemoryObjectReceiveStream[Message]
                send_stream, receive_stream = anyio.create_memory_object_stream(max_buffer_size=math.inf)

                own_consumer: _Consumer | None = None
                if not self._send_only:
                    own_topic_path = await self._ensure_topic(publisher, self._direct_topic_id(self.address))
                    own_subscription_path = await self._ensure_subscription(subscriber, self.address, own_topic_path)

                    own_future = self._start_streaming_pull(subscriber, own_subscription_path, portal, send_stream)
                    own_consumer = _Consumer(subscription_path=own_subscription_path, future=own_future)
            except BaseException:
                # A partial start must not leak the clients. `portal.__aexit__` closes
                # over the cancel scope `portal.__aenter__` opened above, outside any
                # shield - nesting a fresh shielded scope around it and exiting that
                # older scope from within violates anyio's cancel-scope stack ordering
                # ("Attempted to exit a cancel scope that isn't the current task's
                # current cancel scope"), so it must run unshielded here.
                await portal.__aexit__(None, None, None)
                with anyio.CancelScope(shield=True):
                    await run_sync(subscriber.close)
                publisher.stop()
                raise

            state = _StartedState(
                publisher=publisher,
                subscriber=subscriber,
                own_consumer=own_consumer,
                topic_consumers={},
                portal=portal,
                send_stream=send_stream,
                receive_stream=receive_stream,
            )

            if self._health_check_task is not None:
                await self._health_check_task.start()

            self._state = state
            return state

    async def _ensure_topic(self, publisher: PublisherClient, topic_id: str) -> str:
        topic_path: str = publisher.topic_path(self._project_id, topic_id)
        if topic_id in self._ensured_topic_ids:
            return topic_path
        if self._should_declare_topics:
            with suppress(AlreadyExists):
                await run_sync(partial(publisher.create_topic, name=topic_path))
        self._ensured_topic_ids.add(topic_id)
        return topic_path

    async def _ensure_subscription(self, subscriber: SubscriberClient, subscription_id: str, topic_path: str) -> str:
        subscription_path: str = subscriber.subscription_path(self._project_id, subscription_id)
        if self._should_declare_subscriptions:
            with suppress(AlreadyExists):
                await run_sync(
                    partial(
                        subscriber.create_subscription,
                        name=subscription_path,
                        topic=topic_path,
                        ack_deadline_seconds=self._ack_deadline_seconds,
                    )
                )
        return subscription_path

    def _start_streaming_pull(
        self,
        subscriber: SubscriberClient,
        subscription_path: str,
        portal: BlockingPortal,
        send_stream: MemoryObjectSendStream[Message],
    ) -> StreamingPullFuture:
        """Start a pull consumer whose deliveries land on `send_stream`.

        `SubscriberClient.subscribe`'s callback runs on a foreign thread (one of the
        client's own internal threads), so it can't touch an anyio object directly.
        `portal.call` safely schedules `send_stream.send_nowait` onto the event loop
        that owns the stream and blocks the calling (foreign) thread until it's done -
        the purpose-built version of a thread-safe hand-off queue plus pump task.
        """

        def _on_message(message: Message) -> None:
            portal.call(send_stream.send_nowait, message)

        return subscriber.subscribe(subscription_path, callback=_on_message, flow_control=self._flow_control)

    async def _stop_consumer(self, consumer: _Consumer) -> None:
        consumer.future.cancel()
        with suppress(Exception):
            await run_sync(consumer.future.result)

    async def _check_consumers_health(self) -> None:
        """Restart any consumer whose streaming pull has died.

        The client library already retries transient stream failures internally, so a
        done future here means it gave up on something fatal. There's no equivalent of
        the RabbitMQ transport's reactive self-heal on `receive`: multiple independent
        consumers feed the same stream here, so one dying doesn't stop `receive` from
        returning messages the others deliver. This periodic check is the only
        mechanism that notices.
        """
        state = self._state
        if state is None:
            return
        async with self._topic_consumer_lock:
            if state.own_consumer is not None and state.own_consumer.future.done():
                self._logger.warning("gcp_pubsub.consumer.reinitializing", address=self.address)
                state.own_consumer = self._restart_consumer(state, state.own_consumer)
            for topic, consumer in list(state.topic_consumers.items()):
                if consumer.future.done():
                    self._logger.warning("gcp_pubsub.consumer.reinitializing", address=self.address, topic=topic)
                    state.topic_consumers[topic] = self._restart_consumer(state, consumer)

    def _restart_consumer(self, state: _StartedState, consumer: _Consumer) -> _Consumer:
        future = self._start_streaming_pull(
            state.subscriber, consumer.subscription_path, state.portal, state.send_stream
        )
        return _Consumer(subscription_path=consumer.subscription_path, future=future)

    async def _resolve_publish_topic_path(self, state: _StartedState, destination_address: str) -> str:
        """Resolve which GCP topic a destination address publishes to, ensuring it exists.

        A plain address (e.g. ``"billing"``) is a point-to-point destination, published
        to that app's own direct topic. An address of the form ``"topic@marker"`` - as
        produced by
        `mersal_gcp_pubsub.subscription_storage.GCPPubSubSubscriptionStorage.get_subscriber_addresses`
        for pub/sub - is published to the shared event topic for ``topic``; the marker
        itself is only ever `event_topic_prefix` and isn't otherwise inspected.

        Unlike RabbitMQ's topic exchange - a single resource declared once upfront, so
        publishing an event nobody has subscribed to yet trivially succeeds - each GCP
        event topic is its own resource that must exist before anyone can publish to
        it, and no subscriber may have registered (and thus created it) yet. So a
        destination topic is ensured here, on every publish, rather than only when a
        subscriber is registered - subject to `should_declare_topics`, same as any
        other topic this transport manages.
        """
        if "@" in destination_address:
            topic, _marker = destination_address.rsplit("@", 1)
            topic_id = self._event_topic_id(topic)
        else:
            topic_id = self._direct_topic_id(destination_address)
        return await self._ensure_topic(state.publisher, topic_id)

    async def _publish(self, state: _StartedState, topic_path: str, transport_message: TransportMessage) -> None:
        attributes = {key: str(value) for key, value in transport_message.headers.items()}
        colliding_keys = _RESERVED_PUBLISH_ATTRIBUTE_KEYS.intersection(attributes)
        if colliding_keys:
            raise ValueError(
                f"Header key(s) {sorted(colliding_keys)} collide with `publish()`'s own "
                "keyword arguments (ordering_key/retry/timeout) and would be silently "
                "bound to those instead of becoming message attributes; rename the header(s)."
            )
        future = state.publisher.publish(topic_path, transport_message.body, **attributes)
        await run_sync(future.result)

    def _to_transport_message(self, message: Message) -> TransportMessage:
        return TransportMessage(body=message.data, headers=MessageHeaders(dict(message.attributes)))
