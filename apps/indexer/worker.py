import asyncio
import hashlib
import json
import logging
from typing import Any, Optional, Tuple

from kafka import KafkaConsumer
from pydantic import ValidationError

from apps.indexer.engine import IndexingService
from libs.core.config import settings
from libs.schemas.models import AtlasEvent, Document, EventType
from services.event_bus.bus import EventBus
from services.event_bus.topics import EventTopic

logger = logging.getLogger("atlas.indexer.worker")

# Errors that indicate a broken message rather than a flaky dependency:
# retrying those can never succeed, so they go straight to the dead-letter topic.
PERMANENT_ERRORS: Tuple[type, ...] = (
    ValidationError,
    KeyError,
    TypeError,
    ValueError,
    UnicodeError,
)


def _json_safe(value: Any) -> Any:
    """Best-effort JSON-safe representation of an arbitrary Kafka payload."""
    if value is None:
        return None
    try:
        return json.loads(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return {"_unserializable": str(value)[:2000]}


async def publish_dead_letter(raw_value: Any, events: EventBus, error: BaseException) -> None:
    """Route an unprocessable message to the dead-letter topic.

    Raises if the dead-letter publish itself fails so callers can keep the
    source offset uncommitted and let the broker redeliver the message.
    """
    raw_repr = json.dumps(_json_safe(raw_value), default=str)
    dlq_id = hashlib.sha256(f"dlq:{raw_repr}".encode("utf-8")).hexdigest()
    await events.publish(
        EventTopic.DEAD_LETTER.value,
        AtlasEvent(
            event_id=f"dlq:{dlq_id[:24]}",
            type=EventType.CRAWL_FAILED,
            payload={"error": str(error)[:2000], "raw_event": _json_safe(raw_value)},
            source="indexer",
        ),
    )


async def process_message(
    raw_value: Any,
    index: IndexingService,
    events: EventBus,
    attempt: int = 1,
    max_attempts: int = 1,
) -> Optional[bool]:
    """Validate and index a single event message.

    Returns ``True`` when the document was indexed, ``False`` when the message
    was terminally dead-lettered or intentionally ignored, and ``None`` when a
    transient failure should be retried while attempts remain.
    """
    try:
        event = AtlasEvent.model_validate(raw_value)
    except PERMANENT_ERRORS as exc:
        logger.error("Rejecting malformed indexer event: %s", exc)
        await publish_dead_letter(raw_value, events, exc)
        return False

    if event.type != EventType.EXTRACTION_COMPLETED:
        # Not our event type: acknowledge without indexing.
        return False

    try:
        document = Document.model_validate(event.payload.get("document"))
    except PERMANENT_ERRORS as exc:
        logger.error("Rejecting event %s with invalid document: %s", event.event_id, exc)
        await publish_dead_letter(raw_value, events, exc)
        return False

    try:
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
    except PERMANENT_ERRORS as exc:
        logger.error("Document %s cannot be indexed: %s", document.url, exc)
        await publish_dead_letter(raw_value, events, exc)
        return False
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if attempt < max_attempts:
            logger.warning(
                "Transient indexing failure for %s (attempt %d/%d): %s",
                document.url,
                attempt,
                max_attempts,
                exc,
            )
            return None
        logger.exception("Indexing failed permanently for %s: %s", document.url, exc)
        await publish_dead_letter(raw_value, events, exc)
        return False


def _safe_deserialize(raw_bytes: bytes) -> Any:
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:
        return {"_malformed_payload": raw_bytes.decode("utf-8", errors="replace"), "_error": str(exc)}


async def _handle_record(raw_value: Any, index: IndexingService, events: EventBus) -> Optional[bool]:
    """Process one record with bounded in-process retries for transient errors."""
    max_attempts = max(1, settings.INDEXER_MAX_ATTEMPTS)
    backoff = max(0.1, settings.INDEXER_RETRY_BACKOFF)
    for attempt in range(1, max_attempts + 1):
        outcome = await process_message(
            raw_value, index, events, attempt=attempt, max_attempts=max_attempts
        )
        if outcome is not None:
            return outcome
        await asyncio.sleep(backoff * (2 ** (attempt - 1)))
    return None


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
            retry_later = False
            for topic_partition, messages in records.items():
                for message in messages:
                    try:
                        outcome = await _handle_record(message.value, index, events)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        # Dead-letter publish failed (or another unexpected
                        # error): leave the offset uncommitted and rewind so the
                        # broker redelivers this record instead of dropping it.
                        logger.exception(
                            "Message handling failed; rewinding offset %s:%d for redelivery",
                            topic_partition.topic,
                            message.offset,
                        )
                        outcome = None

                    if outcome is None:
                        await asyncio.to_thread(consumer.seek, topic_partition, message.offset)
                        await asyncio.sleep(max(0.1, settings.INDEXER_RETRY_BACKOFF))
                        retry_later = True
                        break
                    # Commit only after the record was indexed or dead-lettered.
                    await asyncio.to_thread(consumer.commit)
                if retry_later:
                    break
    except asyncio.CancelledError:
        raise
    finally:
        await asyncio.to_thread(consumer.close)
        await events.close()
        await index.close()


if __name__ == "__main__":
    asyncio.run(run())
