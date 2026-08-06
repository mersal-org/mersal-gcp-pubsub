Usage
=====

Installation
------------

If you're using `uv`:

.. code-block:: bash

    uv add mersal_gcp_pubsub

Otherwise,

.. code-block:: bash

    pip install mersal_gcp_pubsub

Authentication
---------------

This library uses the standard ``google-cloud-pubsub`` client under the hood, so the
usual Google Cloud authentication rules apply: set the ``GOOGLE_APPLICATION_CREDENTIALS``
environment variable to a service account key file, or otherwise configure `Application
Default Credentials <https://cloud.google.com/docs/authentication/application-default-credentials>`_.
For local development, point the ``PUBSUB_EMULATOR_HOST`` environment variable at a
running `Pub/Sub emulator <https://cloud.google.com/pubsub/docs/emulator>`_ instead - the
client library picks it up automatically, no extra configuration needed.

Configuring the transport & pub/sub
-------------------------------------

Use the plugin :py:class:`~mersal.gcp_pubsub.plugin.GCPPubSubPlugin`, to configure both the transport and pub/sub:

.. code-block:: python

    from mersal.core.app import Mersal
    from mersal.gcp_pubsub.plugin import GCPPubSubPluginConfig

    gcp_pubsub_plugin_config = GCPPubSubPluginConfig(
        project_id="my-gcp-project",
        input_queue_name="my-app",
    )

    app = Mersal(
        "my-app",
        activator,
        plugins=[gcp_pubsub_plugin_config.plugin()],
    )

    await app.start()

Key ``GCPPubSubPluginConfig`` fields:

``project_id``
    The GCP project everything is created in.

``input_queue_name``
    This app's own address - the local id of the topic and subscription it consumes
    from.

``max_outstanding_messages`` (default ``50``)
    How many unacknowledged messages a single pull consumer will hold locally before
    pausing delivery. Analogous to the RabbitMQ transport's ``prefetch_count``.

``ack_deadline_seconds`` (default ``60``)
    The subscription's ack deadline: how long Pub/Sub waits for an ack before
    redelivering a message. A dead-puller safety net, not a processing-time budget -
    while this transport is alive it keeps extending the deadline regardless of this
    value; it only matters once there's no one left to extend it (e.g. a puller that
    crashed without nacking).

``max_lease_duration_seconds`` (default ``3600``)
    The processing-time budget: how long since receiving a message the client library
    keeps auto-extending its ack deadline before giving up and letting it lapse for
    redelivery. Size this to the longest a message may legitimately take to process.

``consumer_health_check_interval`` (default ``60`` seconds)
    How often to check whether a pull consumer's underlying stream has died and
    restart it if so. ``None`` disables the check entirely.

``should_declare_topics`` / ``should_declare_subscriptions`` (default ``True``)
    Let the transport manage its own topics/subscriptions. Set to ``False`` if that
    piece of topology is managed externally instead (e.g. by infrastructure-as-code).

``direct_topic_prefix`` / ``event_topic_prefix`` (default ``"mersal-direct-"`` / ``"mersal-topic-"``)
    Prefixes used to derive GCP topic ids from Mersal addresses/topics - see
    :doc:`implementation_details` for why these exist. If you're also using
    :py:class:`~mersal.gcp_pubsub.subscription_storage.GCPPubSubSubscriptionStorage`
    directly (rather than through this plugin), its ``event_topic_prefix`` must match
    this one.

If for any reason you don't want to use the plugin, the transport can be configured
separately by providing an instance of
:py:class:`~mersal.gcp_pubsub.transport.GCPPubSubTransport` to the ``transport``
argument in the Mersal app constructor. Similarly, an instance of
:py:class:`~mersal.gcp_pubsub.subscription_storage.GCPPubSubSubscriptionStorage` can be
given to the ``subscription_config`` argument - in which case, remember to pass it this
app's own transport (see the class docstring for why).

Push delivery
--------------

GCP Pub/Sub also supports push delivery (Pub/Sub calling an HTTP endpoint you host,
rather than your app pulling). This library currently only implements pull delivery;
push support is planned as a future addition.
