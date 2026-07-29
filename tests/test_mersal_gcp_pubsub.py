from mersal_gcp_pubsub import hello

__all__ = ("test_hello",)


def test_hello() -> None:
    assert hello() == "Hello from mersal-gcp-pubsub!"
