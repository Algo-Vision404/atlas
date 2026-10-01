import asyncio
import json
import logging

from kafka import KafkaConsumer

from apps.indexer.engine import IndexingService
from libs.core.config import settings
from libs.schemas.models import AtlasEvent, Document, EventType
from services.event_bus.bus import EventBus
from services.event_bus.topics import EventTopic

logger = logging.getLogger("atlas.indexer.worker")


async def run():
    index = IndexingService()
    events = EventBus()
    await index.initialize()

    consumer = KafkaConsumer(
        EventTopic.EXTRACTION_COMPLETED.value,
        bootstrap_servers=[v.strip() for v in settings.KAFKA_SERVERS.split(",") if v.strip()],
        group_id="atlas-indexer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    try:
        while True:
            records = await asyncio.to_thread(consumer.poll, 1000)
            for _, messages in records.items():
                for message in messages:
                    event = AtlasEvent.model_validate(message.value)
                    if event.type != EventType.EXTRACTION_COMPLETED:
                        await asyncio.to_thread(consumer.commit)
                        continue

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
                    await asyncio.to_thread(consumer.commit)
    except asyncio.CancelledError:
        raise
    finally:
        await asyncio.to_thread(consumer.close)
        await events.close()
        await index.keyword_index.close()
        await index.vector_index.close()


if __name__ == "__main__":
    asyncio.run(run())
