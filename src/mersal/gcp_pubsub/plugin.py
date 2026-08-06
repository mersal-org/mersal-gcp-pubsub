from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from mersal.gcp_pubsub.subscription_storage import (
    GCPPubSubSubscriptionStorage,
    GCPPubSubSubscriptionStorageConfig,
)
from mersal.gcp_pubsub.transport import GCPPubSubTransport, GCPPubSubTransportConfig
from mersal.lifespan.lifespan_hooks_registration_plugin import LifespanHooksRegistrationPluginConfig
from mersal.logging import Logger
from mersal.plugins import Plugin
from mersal.subscription import SubscriptionStorage
from mersal.threading import AnyIOPeriodicTaskFactory
from mersal.transport.transport import Transport
from mersal.utils.sync import AsyncCallable

if TYPE_CHECKING:
    from mersal.configuration import StandardConfigurator

__all__ = (
    "GCPPubSubPlugin",
    "GCPPubSubPluginConfig",
)


@dataclass
class GCPPubSubPluginConfig:
    project_id: str
    input_queue_name: str
    should_declare_topics: bool = True
    should_declare_subscriptions: bool = True
    direct_topic_prefix: str = "mersal-direct-"
    event_topic_prefix: str = "mersal-topic-"
    ack_deadline_seconds: int = 60
    max_outstanding_messages: int = 50
    max_lease_duration_seconds: float = 3600.0
    consumer_health_check_interval: float | None = 60.0

    def plugin(self) -> GCPPubSubPlugin:
        return GCPPubSubPlugin(self)


class GCPPubSubPlugin(Plugin):
    def __init__(
        self,
        config: GCPPubSubPluginConfig,
    ) -> None:
        self._config = config

    def __call__(self, configurator: StandardConfigurator) -> None:
        def register_transport(configurator: StandardConfigurator) -> GCPPubSubTransport:
            logger = configurator.get(Logger)  # type: ignore[type-abstract]
            transport_config = GCPPubSubTransportConfig(
                project_id=self._config.project_id,
                input_queue_name=self._config.input_queue_name,
                send_only=configurator.send_only,
                should_declare_topics=self._config.should_declare_topics,
                should_declare_subscriptions=self._config.should_declare_subscriptions,
                direct_topic_prefix=self._config.direct_topic_prefix,
                event_topic_prefix=self._config.event_topic_prefix,
                ack_deadline_seconds=self._config.ack_deadline_seconds,
                max_outstanding_messages=self._config.max_outstanding_messages,
                max_lease_duration_seconds=self._config.max_lease_duration_seconds,
                consumer_health_check_interval=self._config.consumer_health_check_interval,
            )
            return GCPPubSubTransport(
                transport_config,
                periodic_task_factory=AnyIOPeriodicTaskFactory(logger=logger),
                logger=logger,
            )

        def register_subscription_storage(
            configurator: StandardConfigurator,
        ) -> GCPPubSubSubscriptionStorage:
            transport = cast("GCPPubSubTransport", configurator.get(Transport))  # type: ignore[type-abstract]
            storage_config = GCPPubSubSubscriptionStorageConfig(
                project_id=self._config.project_id,
                event_topic_prefix=self._config.event_topic_prefix,
                should_declare_event_topics=self._config.should_declare_topics,
                ack_deadline_seconds=self._config.ack_deadline_seconds,
            )
            return GCPPubSubSubscriptionStorage(
                config=storage_config,
                transport=transport,
            )

        configurator.register(Transport, register_transport)
        configurator.register(SubscriptionStorage, register_subscription_storage)

        startup_hooks = [
            lambda config: AsyncCallable(config.get(Transport)),
        ]
        shutdown_hooks = [
            lambda config: AsyncCallable(config.get(Transport).close),
        ]
        plugin = LifespanHooksRegistrationPluginConfig(
            on_startup_hooks=startup_hooks,
            on_shutdown_hooks=shutdown_hooks,
        ).plugin
        plugin(configurator)
