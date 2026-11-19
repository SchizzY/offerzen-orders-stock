from collections.abc import Iterator
from contextlib import contextmanager

from loguru import logger


@contextmanager
def captured_logs(name: str | None = None) -> Iterator[list[str]]:
    """Collect loguru messages (from `name` and below, or all), tracebacks included."""
    messages: list[str] = []
    handler_id = logger.add(
        lambda message: messages.append(str(message).rstrip("\n")),
        level="TRACE",
        format="{message}",
        filter=name,
    )
    try:
        yield messages
    finally:
        logger.remove(handler_id)
