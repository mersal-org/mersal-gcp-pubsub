Welcome to Mersal GCP Pub/Sub's documentation!
================================================

**mersal_gcp_pubsub** is the Google Cloud Pub/Sub implementation for Mersal. It allows using GCP Pub/Sub as a transport that also supports Mersal pub/sub.


Quickstart
------------

.. code-block:: bash

    uv add mersal_gcp_pubsub

.. code-block:: python

    from mersal.app import Mersal
    from mersal_gcp_pubsub.plugin import GCPPubSubPluginConfig

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

See :doc:`usage <./usage>` for more info.


.. toctree::
   :titlesonly:
   :caption: Documentation
   :hidden:

   Home <self>
   usage
   implementation_details
   reference


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
