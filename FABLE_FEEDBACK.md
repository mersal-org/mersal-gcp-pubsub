# Review: GCP Pub/Sub transport & subscription storage

Review of the staged `mersal_gcp_pubsub/transport.py` and
`mersal_gcp_pubsub/subscription_storage.py`, from the perspective of an
experienced distributed-systems/messaging engineer with GCP Pub/Sub and
RabbitMQ background. Contracts consulted: `mersal.transport.Transport` /
`BaseTransport`, `mersal.subscription.SubscriptionStorage`,
`mersal.workers.anyio.AnyioWorker`, and the publish/subscribe flow in
`mersal.app.Mersal`, plus the plugin wiring in `mersal_gcp_pubsub/plugin.py`.

## Overall verdict

The topology mapping is the right one for Pub/Sub, and the best design decision
is `get_subscriber_addresses` returning the synthetic `{topic}@{prefix}` marker
so `publish` becomes a **single** publish to the shared event topic with native
fan-out, instead of enumerating N subscribers and doing N point-to-point sends.
That is exactly how to exploit a broker with native fan-out, and it keeps
`is_centralized = True` honest. The push-pull bridge is correctly reasoned
about (thread-safe handoff, event re-arm that never straddles an await),
startup/teardown is careful with cancellation shields, and the comments
explaining *why* things differ from the RabbitMQ transport are genuinely good.
This is a competent integration.

Two issues are blocking; a handful more should be fixed before trusting it
under load.

## Blocking issues

### 1. `create_queue` can silently lose messages — the classic Pub/Sub trap

Pub/Sub discards any message published to a topic with zero subscriptions.
`create_queue` (`transport.py:239`) and the direct branch of
`_resolve_publish_topic_path` only ensure the **topic** for a destination
address; the subscription for that address is only created when the owning app
itself starts (`_ensure_started`). So "app A sends to app B before B has ever
booted" drops the message with a *successful* publish.

In RabbitMQ, declaring the queue is what gives you retention; here the
subscription **is** the retention, and it's the one thing `create_queue`
doesn't create.

**Fix:** `create_queue` should create both the direct topic *and* the
`{address}` subscription on it, so point-to-point sends to a not-yet-started
peer are buffered like a declared AMQP queue would be.

(The same trap exists for events published before any subscriber registers, but
that is legitimate pub/sub semantics; for point-to-point it's a correctness
bug.)

### 2. An admin-plane RPC on every single publish

`_resolve_publish_topic_path` calls `_ensure_topic` per message, and with
`should_declare_topics=True` (the default) that's a `create_topic` call —
expecting `AlreadyExists` — on **every publish** (`transport.py:457-480`).
Admin operations in Pub/Sub are rate-limited separately (and much lower) than
data-plane publishes, and each one adds a full RPC round trip to publish
latency. Under real throughput this hits admin quota errors from the hot path.

**Fix:** cache ensured topic IDs in a `set[str]` and only create once per topic
per process. This is the equivalent of calling `exchange_declare` per
`basic.publish` in AMQP — nobody does that.

## Worth fixing

### 3. Sequential publish defeats the client's batching

`send_outgoing_messages` publishes and awaits `future.result()` one message at
a time. The publisher client batches (default ~10 ms linger), so each message
pays linger + round trip serially. Fire all `publish()` calls for the outgoing
batch first, then await all futures — outbox-style commits with several
messages will feel the difference.

### 4. `asyncio.create_task` in an otherwise anyio codebase

`transport.py:361` pins the transport to the asyncio backend, while everything
else — locks, events, `run_sync`, and Mersal's own worker — is anyio.

More broadly, the whole `SimpleQueue` + pump thread + `deque` + event-swap
contraption could be replaced with an anyio **memory object stream** plus a
**`BlockingPortal`**: the GCP callback thread does
`portal.call(send_stream.send_nowait, msg)`, and `receive` is just
`await receive_stream.receive()`. That deletes the pump task, the `_STOP`
sentinel, the hand-rolled `_next_message` event re-arm logic (correct, but
exactly the kind of subtle concurrency code you want to not have), and the
worker thread permanently parked on `SimpleQueue.get` — which currently
occupies one of anyio's default 40 thread tokens for the transport's whole
lifetime. (See appendix below for how the portal works.)

### 5. `close()` strands buffered messages until the ack deadline

Messages sitting in `state.buffer` and the handoff queue at shutdown are
neither acked nor nacked, so they stay invisible to other consumers for up to
`ack_deadline_seconds`. On close, drain both and `nack()` each for prompt
redelivery — the Pub/Sub equivalent of AMQP requeue-on-channel-close, except
here it has to be done explicitly. (The RabbitMQ transport already does this in
`_close_consumer`.)

### 6. Header keys can collide with `publish()` kwargs

`publisher.publish(topic_path, body, **attributes)` (`transport.py:484`) means
a user header named `timeout`, `retry`, or `ordering_key` gets swallowed as a
keyword argument to `publish` instead of becoming an attribute — a header
`"timeout": "30"` would be interpreted as the publish RPC timeout. Low
probability, very confusing when it happens.

## Document (at minimum)

- **Attribute limits:** Pub/Sub allows max 100 attributes, keys ≤ 256 bytes,
  values ≤ 1024 bytes, and keys can't start with `goog`. Mersal error-handling
  flows that stuff exception details into headers will fail to publish. The
  string-coercion lossiness is already documented (good); the size limits are
  the sharper edge. If headers can be large, an envelope-in-body encoding is
  the safer design.
- **Resource-ID legality and the `--` convention:** GCP topic/subscription IDs
  allow only `[A-Za-z0-9-_.~+%]`, ≤ 255 chars. Mersal topic names derived from
  types may contain characters that aren't legal, and the `{address}--{topic}`
  scheme collides if an address itself contains `--` (app `a--b` on topic `c`
  vs app `a` on topic `b--c`). A small sanitize/validate step would turn a
  cryptic `InvalidArgument` into a clear error.
- **Thread cost per subscription:** each `subscriber.subscribe()` call spins up
  its own scheduler/executor (roughly 10 callback threads plus stream
  machinery). An app subscribed to many topics gets thread-heavy fast — the
  opposite of RabbitMQ's one-connection-many-channels model. Passing a shared
  `scheduler` to `subscribe()` caps that.
- **`unregister_subscriber` deletes the subscription**, discarding the
  subscriber's backlog — different from an AMQP unbind, which leaves the
  queue's contents alone. Correct semantics for "unsubscribe", but worth a doc
  note.

## Smaller observations

- `_RETRY_DELAYS = [0.0, 0.0]` — two immediate retries on top of the client
  library's own api-core retry policy adds essentially nothing; if a retrier is
  kept, give it backoff.
- `message.ack()`/`nack()` are non-blocking (they enqueue onto the streaming
  pull manager's dispatcher), so the `run_sync` wrap in the ack/nack callbacks
  is unnecessary thread-hop overhead.
- The health-check design is sound, and the reasoning for why RabbitMQ-style
  reactive self-heal doesn't work here is exactly right. Nit: `close()` must
  fully await the periodic task's stop before tearing down consumers, or the
  check could restart one mid-close — it does call `stop()` first; just make
  sure `stop()` waits for an in-flight run.
- Lease management works in the transport's favor: the client auto-extends
  deadlines (modack) for messages held in the local buffer up to
  `FlowControl.max_lease_duration` (default 1 hour), and buffered messages
  count against `max_outstanding_messages`, so backpressure is real. Consider
  exposing `max_lease_duration` in the config — it's the true "how long can
  processing take" knob, not `ack_deadline_seconds`.
- Fine to punt on, but state in docs: ordering keys and exactly-once delivery
  (`ack_with_response`) are unsupported; delivery is at-least-once, unordered.

## Priorities

Fix **#1** and **#2** before this ships anywhere real — one loses messages
silently, the other falls over at moderate throughput. #3–#6 are a solid
follow-up pass, and the anyio-stream refactor in #4 also makes the trickiest
concurrency code in the file disappear rather than need defending in comments.

---

## Appendix: the BlockingPortal approach (issue 4)

The problem being solved: anyio/asyncio objects (events, streams, locks) may
only be touched from the thread running the event loop. The GCP Pub/Sub client
delivers messages by invoking a callback **on its own internal thread pool** —
a foreign thread from the event loop's point of view. The current code bridges
that gap with a thread-safe `queue.SimpleQueue` plus a dedicated pump task that
blocks a worker thread on `queue.get` to ferry items back into async land.

`anyio.from_thread.BlockingPortal` is the purpose-built version of that bridge.
Created *inside* the event loop, it can be handed to any foreign thread, and
`portal.call(fn, *args)` safely schedules `fn` onto the event loop and waits
for it. The GCP callback (running on Google's thread) becomes:

```python
portal.call(send_stream.send_nowait, message)
```

where `send_stream` / `receive_stream` are an anyio memory object stream pair.
`receive()` on the transport side is then just
`await receive_stream.receive()`. The pump task, the sentinel, the deque, the
manual event re-arm, and the permanently-parked thread all disappear — the
portal and the stream are the whole bridge, and both are backend-neutral
(asyncio *and* trio).

Key point: a portal is only needed when a **foreign thread** must call into the
event loop. It is *not* applicable to async-native libraries (e.g. aio-pika in
the RabbitMQ transport), where messages already arrive on the event loop
itself — there is no thread boundary to bridge there.
