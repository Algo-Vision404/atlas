import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from kafka import KafkaProducer

from libs.core.config import settings
from libs.schemas.models import AtlasEvent

logger = logging.getLogger("atlas.event_bus")


class EventBus:
    """Kafka-compatible event publisher with non-blocking async boundaries."""

    def __init__(self, bootstrap_servers: str = settings.KAFKA_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[KafkaProducer] = None
        self.published_events: List[Tuple[str, Dict[str, Any]]] = []

    def _ensure_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=[
                    server.strip() for server in self.bootstrap_servers.split(",") if server.strip()
                ],
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=10,
            )
        return self._producer

    def _send_sync(self, topic: str, payload: Dict[str, Any]) -> None:
        producer = self._ensure_producer()
        future = producer.send(topic, payload)
        if hasattr(future, "get"):
            future.get(timeout=10)

    async def publish(self, topic: str, event: AtlasEvent) -> None:
        payload = event.model_dump(mode="json")
        if settings.MOCK_MODE:
            self.published_events.append((topic, payload))
            logger.info("MOCK EventBus: published %s to %s", event.event_id, topic)
            return

        try:
            await asyncio.to_thread(self._send_sync, topic, payload)
        except Exception:
            logger.exception("Failed to publish event to %s", topic)
            raise

    async def flush(self) -> None:
        if settings.MOCK_MODE or self._producer is None:
            return
        await asyncio.to_thread(self._producer.flush)

    async def close(self) -> None:
        if self._producer is not None:
            await asyncio.to_thread(self._producer.flush)
            await asyncio.to_thread(self._producer.close)
            self._producer = None
