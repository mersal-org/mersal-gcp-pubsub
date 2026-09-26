from datetime import timedelta

import pytest

from mersal.activation import BuiltinHandlerActivator
from mersal.core.app import Mersal
from mersal.exceptions import DeferralNotSupportedError
from mersal.gcp_pubsub.plugin import GCPPubSubPluginConfig
from mersal.gcp_pubsub.transport import GCPPubSubTransport, GCPPubSubTransportConfig
from mersal.serialization.identity_serializer import IdentitySerializer

__all__ = ("TestGCPPubSubDeferral",)


pytestmark = pytest.mark.anyio


class TestGCPPubSubDeferral:
    """Pub/Sub can't schedule delivery, so without a timeout manager deferring must
    fail before anything is sent - no emulator needed, since the transport connects
    lazily."""

    async def test_transport_does_not_support_deferral(self) -> None:
        transport = GCPPubSubTransport(GCPPubSubTransportConfig(project_id="test-project", input_queue_name="q"))

        assert not transport.supports_deferral

    async def test_app_refuses_to_defer_without_a_timeout_manager(self) -> None:
        app = Mersal(
            "gcp-defer-test-app",
            BuiltinHandlerActivator(),
            plugins=[GCPPubSubPluginConfig(project_id="test-project", input_queue_name="q").plugin()],
            serializer=IdentitySerializer(),
        )

        with pytest.raises(DeferralNotSupportedError):
            await app.defer_local(timedelta(seconds=1), object())
