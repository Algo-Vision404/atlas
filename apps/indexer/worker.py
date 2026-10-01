import asyncio
import hashlib
import json
import logging
from typing import Any

from kafka import KafkaConsumer

from apps.indexer.engine import IndexingService
from libs.core.config import settings
from libs.schemas.models import AtlasEvent, Document, EventType
from services.event_bus.bus import EventBus
from services.event_bus.topics import EventTopic

logger = logging.getLogger("atlas.indexer.worker")


async def process_message(raw_value: Any, index: IndexingService, events: EventBus) -> bool:
    """Validate and index a single event message, routing poison pills to the dead-letter topic."""
    try:
        event = AtlasEvent.model_validate(raw_value)
        if event.type != EventType.EXTRACTION_COMPLETED:
            return False

        document = Document.model_validate(event.payload["document"])
        await index.index_document(document)
        await events.publish(
            EventTopic.INDEXING_COMPLETED.value,
            AtlasEvent(
                event_id=f"indexed:{event.event_id}",
                type=EventType.INDEXING_COMPLETED,
                payload={"document_id": document.id, "url": document.url},
                source="indexer",
            ),
        )
        return True
    except Exception as exc:
        logger.exception("Failed to process indexer message: %s", exc)
        try:
            raw_repr = json.dumps(raw_value, default=str) if not isinstance(raw_value, str) else raw_value
            dlq_id = hashlib.sha256(f"dlq:{raw_repr}".encode("utf-8")).hexdigest()
            await events.publish(
                EventTopic.DEAD_LETTER.value,
                AtlasEvent(
                    event_id=f"dlq:{dlq_id[:24]}",
                    type=EventType.CRAWL_FAILED,
                    payload={"error": str(exc), "raw_event": raw_value},
                    source="indexer",
                ),
            )
        except Exception:
            logger.exception("Failed to publish message to dead-letter topic")
        return False


def _safe_deserialize(raw_bytes: bytes) -> Any:
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:
        return {"_malformed_payload": raw_bytes.decode("utf-8", errors="replace"), "_error": str(exc)}


async def run():
    index = IndexingService()
    events = EventBus()
    await index.initialize()

    if settings.MOCK_MODE:
        logger.info("MOCK Indexer worker started")
        try:
            while True:
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        finally:
            await events.close()
            await index.close()
        return

    consumer = KafkaConsumer(
        EventTopic.EXTRACTION_COMPLETED.value,
        bootstrap_servers=[v.strip() for v in settings.KAFKA_SERVERS.split(",") if v.strip()],
        group_id="atlas-indexer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1,
        value_deserializer=_safe_deserialize,
    )

    try:
        while True:
            records = await asyncio.to_thread(consumer.poll, 1000)
            for _, messages in records.items():
                for message in messages:
                    await process_message(message.value, index, events)
                    await asyncio.to_thread(consumer.commit)
    except asyncio.CancelledError:
        raise
    finally:
        await asyncio.to_thread(consumer.close)
        await events.close()
        await index.close()


if __name__ == "__main__":
    asyncio.run(run())
