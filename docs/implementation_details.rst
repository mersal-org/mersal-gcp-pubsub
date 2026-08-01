Implementation Details
==========================

Client library
~~~~~~~~~~~~~~~~

`google-cloud-pubsub <https://cloud.google.com/python/docs/reference/pubsub/latest>`_
is used as the GCP Pub/Sub client library. Unlike ``aio-pika`` (used by
``mersal_rabbitmq``), it isn't asyncio-native - it's a thread/callback-based client
built on gRPC - so every blocking call is bridged onto a worker thread via
``anyio.to_thread.run_sync``.

Topology
~~~~~~~~~~

GCP Pub/Sub has no equivalent of AMQP's exchanges or routing keys: publishing always
means publishing to a named **topic**, and consuming always means pulling from a
**subscription** bound to exactly one topic. So, unlike the RabbitMQ transport's two
shared exchanges, every Mersal address becomes its own topic:

- A plain point-to-point address (e.g. ``"billing"``) becomes topic
  ``f"{direct_topic_prefix}billing"``. Each app's own input queue is backed by a single
  subscription (named after its address) bound to its own direct topic.
- A pub/sub topic name (e.g. ``"order.created"``) becomes topic
  ``f"{event_topic_prefix}order.created"``. Every subscriber gets its own subscription
  on that *same* topic (see
  :py:class:`~mersal_gcp_pubsub.subscription_storage.GCPPubSubSubscriptionStorage`), so
  publishing once still reaches everyone subscribed - broker-native fan-out, just
  modeled as N subscriptions on one topic rather than N bindings on one exchange.

Because each event topic is its own resource - unlike RabbitMQ's topic exchange, a
single resource declared once upfront - publishing an event nobody has subscribed to
yet would otherwise fail with ``NotFound``. To avoid that, sending ensures the
destination topic exists (subject to ``should_declare_topics``) on every publish,
rather than only when a subscriber first registers.

Message attributes are always a plain ``map<string, string>`` on the wire, unlike
AMQP's richer field-table headers - so every Mersal header value round-trips as a
``str``, regardless of what type it was sent as.

Mersal integration
~~~~~~~~~~~~~~~~~~~~

The library provides an implementation for Mersal's two main protocols:

- :py:class:`mersal.transport.Transport`, via
  :py:class:`~mersal_gcp_pubsub.transport.GCPPubSubTransport`
- :py:class:`mersal.subscription.SubscriptionStorage`, via
  :py:class:`~mersal_gcp_pubsub.subscription_storage.GCPPubSubSubscriptionStorage`

Push-Pull bridge
-------------------

Mersal's worker is designed to be a pull-based API: an infinite loop queries the
transport for messages, one at a time, up to ``max_parallelism`` concurrently in
flight.

This doesn't align well with how ``google-cloud-pubsub``'s subscriber client actually
works: ``SubscriberClient.subscribe`` delivers messages by invoking a callback on a
thread pool it manages internally - a push model, the same shape of problem the
RabbitMQ transport solves for ``aio-pika``'s async consumer, just thread-based instead
of asyncio-based.

The bridge here: every subscription's callback runs on a foreign thread as far as the
event loop is concerned, so it hands each message off via an
``anyio.from_thread.BlockingPortal`` - created once when the transport starts, inside
the event loop it will bridge into. The callback does
``portal.call(send_stream.send_nowait, message)`` on a shared anyio memory object
stream; ``receive`` is just ``await receive_stream.receive()``. ``receive`` is not
allowed to return ``None`` when there are no messages - it awaits a message arriving
on the stream, same as the RabbitMQ transport.

One portal and one stream pair serve *every* subscription this transport consumes -
this app's own, plus one per pub/sub topic it's subscribed to - since the hand-off is
already thread-safe.

Dynamic pub/sub subscriptions
--------------------------------

Subscribing to a new topic at runtime is a bigger deal here than in RabbitMQ. In
RabbitMQ, a new binding on an already-running queue needs no new consumer - the same
physical queue just starts receiving more. In GCP Pub/Sub, a subscription is always
bound to exactly one topic, so subscribing this app to a new topic genuinely means
creating a new ``Subscription`` resource *and* starting a new pull consumer for it.

This is why
:py:class:`~mersal_gcp_pubsub.subscription_storage.GCPPubSubSubscriptionStorage` needs
a reference to the subscribing app's own
:py:class:`~mersal_gcp_pubsub.transport.GCPPubSubTransport`: when that transport's own
address registers or unregisters itself, the storage starts or stops the matching live
pull consumer via
:py:meth:`~mersal_gcp_pubsub.transport.GCPPubSubTransport.start_consuming_topic` /
:py:meth:`~mersal_gcp_pubsub.transport.GCPPubSubTransport.stop_consuming_topic`, feeding
newly-arriving messages into the same shared stream as everything else.

Unlike an AMQP unbind, unregistering a subscriber **deletes** its underlying GCP
subscription - taking that subscriber's undelivered backlog with it, rather than
leaving a shared queue's contents alone. This is the correct semantics for "this
subscriber no longer wants this topic's events", but is worth keeping in mind since
it has no RabbitMQ equivalent.

Self-healing
~~~~~~~~~~~~~~

The RabbitMQ transport notices a dead consumer reactively, the next time ``receive``
is called - there's only ever one physical consumer per transport there. That doesn't
generalize here: several independent pull consumers (this app's own, plus one per
subscribed topic) all feed the same stream, so one of them dying doesn't stop
``receive`` from returning messages the others deliver. Instead, a periodic health
check (``consumer_health_check_interval``) restarts any consumer whose underlying
stream has died.

Each ``subscriber.subscribe()`` call spins up its own scheduler and executor threads
(roughly ten callback threads plus stream machinery), so an app subscribed to many
topics gets thread-heavy quickly - the opposite of RabbitMQ's one-connection,
many-channels model. Passing a shared scheduler to ``subscribe()`` would cap this if
it becomes a problem in practice.

Limits and legal identifiers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Message attributes:** Pub/Sub allows at most 100 attributes per message, with keys
  up to 256 bytes and values up to 1024 bytes; attribute keys also can't start with
  ``goog``. Headers that stuff large or numerous values (e.g. exception details from
  error-handling flows) can exceed these and fail the publish outright. If headers may
  be large, prefer encoding that data into the message body instead.
- **Topic/subscription ids:** GCP only allows ``[A-Za-z0-9-_.~+%]``, up to 255
  characters. A Mersal address or topic name containing other characters will fail
  with a cryptic ``InvalidArgument`` rather than a clear error. Names are also joined
  with a literal ``--`` (e.g. ``f"{subscriber_address}--{topic}"``) to derive
  subscription ids, so an address or topic that itself contains ``--`` can collide
  with a different address/topic pair (e.g. app ``a--b`` on topic ``c`` collides with
  app ``a`` on topic ``b--c``). Avoid ``--`` in addresses and topic names.

Missing features
~~~~~~~~~~~~~~~~~~~

1. Push delivery (an HTTP endpoint Pub/Sub calls, rather than this app pulling) - see
   :doc:`usage`.
2. Message ordering keys and exactly-once delivery.
3. Message expiry and deferred messages, same as ``mersal_rabbitmq``.
