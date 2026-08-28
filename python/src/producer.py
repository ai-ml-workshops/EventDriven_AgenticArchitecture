"""Event Producer for emitting task request events into Kafka."""

import json
import logging
from typing import Any, Dict, Optional

from .config import PipelineConfig
from .models import TaskRequestEvent, TaskType

logger = logging.getLogger(__name__)


class EventProducer:
    """Producer that creates and publishes TaskRequestEvents to Kafka."""

    def __init__(self, config: Optional[PipelineConfig] = None, kafka_producer=None):
        self.config = config or PipelineConfig()
        self._external_producer = kafka_producer
        self._producer = None

    def _get_producer(self):
        if self._external_producer is not None:
            return self._external_producer
        if self._producer is None:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.config.kafka_bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
            )
        return self._producer

    def send_event(self, event: TaskRequestEvent) -> Any:
        """Publishes a TaskRequestEvent to Kafka topic."""
        producer = self._get_producer()
        topic = self.config.task_requests_topic
        key = event.correlationId
        value = event.model_dump()

        logger.info(f"Producing event {event.eventId} (correlationId={key}, taskType={event.taskType}) to topic '{topic}'")
        future = producer.send(topic, key=key, value=value)
        if hasattr(future, "get"):
            return future.get(timeout=10)
        return future

    def create_and_send_task(
        self,
        prompt: str,
        task_type: TaskType = TaskType.GENERAL_ASSISTANT,
        user_id: str = "system-user",
        context: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
    ) -> TaskRequestEvent:
        """Helper to create and immediately send a TaskRequestEvent."""
        event = TaskRequestEvent.create(
            prompt=prompt,
            task_type=task_type,
            user_id=user_id,
            context=context,
            parameters=parameters,
            correlation_id=correlation_id,
        )
        self.send_event(event)
        return event

    def flush(self):
        producer = self._get_producer()
        if hasattr(producer, "flush"):
            producer.flush()

    def close(self):
        if self._producer is not None and hasattr(self._producer, "close"):
            self._producer.close()
            self._producer = None
