import asyncio
import json
import logging
from typing import Optional

from kafka import KafkaProducer

from libs.core.config import settings
from libs.schemas.models import AtlasEvent

logger = logging.getLogger("atlas.event_bus")


class EventBus:
    """Kafka-compatible event publisher with non-blocking async boundaries."""

    def __init__(self, bootstrap_servers: str = settings.KAFKA_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[KafkaProducer] = None

    def _ensure_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=[server.strip() for server in self.bootstrap_servers.split(",") if server.strip()],
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=10,
            )
        return self._producer

    async def publish(self, topic: str, event: AtlasEvent) -> None:
        payload = event.model_dump(mode="json")
        producer = await asyncio.to_thread(self._ensure_producer)
        await asyncio.to_thread(producer.send, topic, payload)
        await asyncio.to_thread(producer.flush)

    async def close(self) -> None:
        if self._producer is not None:
            await asyncio.to_thread(self._producer.close)
            self._producer = None
