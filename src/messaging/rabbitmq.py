import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustChannel,
    AbstractRobustConnection,
)

MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]


class RabbitPublisher:
    def __init__(self, url: str, queue_name: str) -> None:
        self._url = url
        self._queue_name = queue_name
        self._connection: AbstractRobustConnection | None = None

    async def connect(self, *, attempts: int = 30, delay_seconds: float = 1.0) -> None:
        self._connection = await _connect_with_retry(
            self._url,
            attempts=attempts,
            delay_seconds=delay_seconds,
        )
        channel = await self._connection.channel()
        await channel.declare_queue(self._queue_name, durable=True)
        await channel.close()

    async def publish(self, payload: dict[str, Any]) -> None:
        if self._connection is None:
            await self.connect()

        assert self._connection is not None
        channel = await self._connection.channel()
        try:
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(payload).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                ),
                routing_key=self._queue_name,
            )
        finally:
            await channel.close()

    async def purge_queue(self) -> int:
        if self._connection is None:
            await self.connect()

        assert self._connection is not None
        channel = await self._connection.channel()
        try:
            queue = await channel.declare_queue(self._queue_name, durable=True)
            result = await queue.purge()
            return int(getattr(result, "message_count", 0))
        finally:
            await channel.close()

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None


class RabbitConsumer:
    def __init__(self, url: str, queue_name: str, handler: MessageHandler) -> None:
        self._url = url
        self._queue_name = queue_name
        self._handler = handler
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._queue: AbstractQueue | None = None
        self._consumer_tag: str | None = None

    async def start(self) -> None:
        self._connection = await _connect_with_retry(self._url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=1)
        self._queue = await self._channel.declare_queue(self._queue_name, durable=True)
        self._consumer_tag = await self._queue.consume(self._process_message)

    @property
    def is_connected(self) -> bool:
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
            and self._consumer_tag is not None
        )

    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            payload = json.loads(message.body.decode("utf-8"))
            await self._handler(payload)

    async def close(self) -> None:
        if self._queue is not None and self._consumer_tag is not None:
            await self._queue.cancel(self._consumer_tag)
            self._consumer_tag = None
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._queue = None
        if self._connection is not None:
            await self._connection.close()
            self._connection = None


async def _connect_with_retry(
    url: str,
    *,
    attempts: int = 30,
    delay_seconds: float = 1.0,
) -> AbstractRobustConnection:
    if attempts < 1:
        raise ValueError("attempts must be positive")

    for attempt in range(1, attempts + 1):
        try:
            return await aio_pika.connect_robust(url)
        except (OSError, aio_pika.AMQPException):
            if attempt == attempts:
                raise
            await asyncio.sleep(delay_seconds)

    raise RuntimeError("RabbitMQ connection retry loop ended unexpectedly")
