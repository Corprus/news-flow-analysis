import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import suppress
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
    """Публикатор JSON-сообщений в durable RabbitMQ-очереди."""

    def __init__(self, url: str, queue_name: str) -> None:
        """Сохранить URL RabbitMQ и основную очередь публикации."""
        self._url = url
        self._queue_name = queue_name
        self._connection: AbstractRobustConnection | None = None

    async def connect(self, *, attempts: int = 30, delay_seconds: float = 1.0) -> None:
        """Подключиться к RabbitMQ с ретраями и объявить основную очередь."""
        self._connection = await _connect_with_retry(
            self._url,
            attempts=attempts,
            delay_seconds=delay_seconds,
        )
        await self.declare_queue(self._queue_name)

    async def declare_queue(self, queue_name: str) -> None:
        """Объявить durable-очередь, создавая подключение при необходимости."""
        if self._connection is None:
            await self.connect()
            return
        channel = await self._connection.channel()
        try:
            await channel.declare_queue(queue_name, durable=True)
        finally:
            await channel.close()

    async def publish(
        self,
        payload: dict[str, Any],
        *,
        queue_name: str | None = None,
    ) -> None:
        """Опубликовать JSON payload в основную или указанную очередь."""
        if self._connection is None:
            await self.connect()

        assert self._connection is not None
        routing_key = queue_name or self._queue_name
        if queue_name is not None:
            await self.declare_queue(queue_name)
        channel = await self._connection.channel()
        try:
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(payload).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                ),
                routing_key=routing_key,
            )
        finally:
            await channel.close()

    async def purge_queue(self, *, queue_name: str | None = None) -> int:
        """Очистить очередь и вернуть число удалённых сообщений, если брокер его сообщил."""
        if self._connection is None:
            await self.connect()

        assert self._connection is not None
        target_queue = queue_name or self._queue_name
        channel = await self._connection.channel()
        try:
            queue = await channel.declare_queue(target_queue, durable=True)
            result = await queue.purge()
            return int(getattr(result, "message_count", 0))
        finally:
            await channel.close()

    async def close(self) -> None:
        """Закрыть активное подключение publisher-а."""
        if self._connection is not None:
            await self._connection.close()
            self._connection = None


class RabbitConsumer:
    """Потребитель JSON-сообщений из RabbitMQ с последовательной обработкой."""

    def __init__(self, url: str, queue_name: str, handler: MessageHandler) -> None:
        """Сохранить параметры очереди и callback обработки сообщений."""
        self._url = url
        self._queue_name = queue_name
        self._handler = handler
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._queue: AbstractQueue | None = None
        self._consumer_tag: str | None = None

    async def start(self) -> None:
        """Запустить consumer с prefetch=1 для контролируемой обработки задач."""
        self._connection = await _connect_with_retry(self._url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=1)
        self._queue = await self._channel.declare_queue(self._queue_name, durable=True)
        self._consumer_tag = await self._queue.consume(self._process_message)

    @property
    def is_connected(self) -> bool:
        """Проверить, что connection, channel и consumer-tag ещё активны."""
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
        """Остановить consumer и закрыть channel/connection."""
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


class RabbitPriorityConsumer:
    """Последовательный consumer, который предпочитает приоритетную очередь."""

    def __init__(
        self,
        url: str,
        *,
        primary_queue_name: str,
        priority_queue_name: str,
        handler: MessageHandler,
    ) -> None:
        """Сохранить параметры основной и приоритетной очередей."""
        self._url = url
        self._primary_queue_name = primary_queue_name
        self._priority_queue_name = priority_queue_name
        self._handler = handler
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._primary_queue: AbstractQueue | None = None
        self._priority_queue: AbstractQueue | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Запустить polling-loop с обработкой не более одного сообщения за раз."""
        self._connection = await _connect_with_retry(self._url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=1)
        self._priority_queue = await self._channel.declare_queue(
            self._priority_queue_name,
            durable=True,
        )
        self._primary_queue = await self._channel.declare_queue(
            self._primary_queue_name,
            durable=True,
        )
        self._task = asyncio.create_task(self._consume_loop())

    @property
    def is_connected(self) -> bool:
        """Проверить, что consumer-loop и AMQP channel активны."""
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
            and self._task is not None
            and not self._task.done()
        )

    async def _consume_loop(self) -> None:
        while True:
            message = await self._get_next_message()
            if message is None:
                await asyncio.sleep(0.2)
                continue
            await self._process_message(message)

    async def _get_next_message(self) -> AbstractIncomingMessage | None:
        assert self._priority_queue is not None
        assert self._primary_queue is not None
        priority_message = await _queue_get_or_none(self._priority_queue)
        if priority_message is not None:
            return priority_message
        return await _queue_get_or_none(self._primary_queue)

    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            payload = json.loads(message.body.decode("utf-8"))
            await self._handler(payload)

    async def close(self) -> None:
        """Остановить polling-loop и закрыть channel/connection."""
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._primary_queue = None
            self._priority_queue = None
        if self._connection is not None:
            await self._connection.close()
            self._connection = None


async def _queue_get_or_none(queue: AbstractQueue) -> AbstractIncomingMessage | None:
    try:
        return await queue.get(timeout=1, fail=False)
    except TimeoutError:
        return None


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
