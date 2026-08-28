"""Subscriber 1: Audit and Analytics Subscriber."""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .config import PipelineConfig
from .models import AgentResponseEvent

logger = logging.getLogger(__name__)


@dataclass
class AuditRecord:
    eventId: str
    correlationId: str
    agentId: str
    taskType: str
    status: str
    model: str
    totalTokens: int
    latencyMs: int
    timestamp: str


@dataclass
class AuditMetrics:
    totalProcessed: int = 0
    successCount: int = 0
    failureCount: int = 0
    totalTokensUsed: int = 0
    totalLatencyMs: int = 0

    @property
    def averageLatencyMs(self) -> float:
        return self.totalLatencyMs / self.totalProcessed if self.totalProcessed > 0 else 0.0


class AuditLogSubscriber:
    """Subscriber that tracks audit logs, telemetry, and analytics for agent responses."""

    def __init__(
        self,
        config: Optional[PipelineConfig] = None,
        kafka_consumer=None,
        kafka_producer=None,
        subscriber_id: str = "audit-analytics-subscriber-python",
    ):
        self.config = config or PipelineConfig()
        self.subscriber_id = subscriber_id
        self._external_consumer = kafka_consumer
        self._external_producer = kafka_producer
        self._consumer = None
        self._producer = None
        self._running = False
        self.audit_records: List[AuditRecord] = []
        self.metrics = AuditMetrics()

    def _get_consumer(self):
        if self._external_consumer is not None:
            return self._external_consumer
        if self._consumer is None:
            from kafka import KafkaConsumer
            self._consumer = KafkaConsumer(
                self.config.agent_responses_topic,
                bootstrap_servers=self.config.kafka_bootstrap_servers.split(","),
                group_id=self.config.audit_consumer_group,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
        return self._consumer

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

    def process_response(self, response: AgentResponseEvent) -> AuditRecord:
        """Processes an AgentResponseEvent, logs structured audit record, and updates metrics."""
        record = AuditRecord(
            eventId=response.eventId,
            correlationId=response.correlationId,
            agentId=response.agentId,
            taskType=response.taskType,
            status=response.status,
            model=response.model,
            totalTokens=response.usage.totalTokens,
            latencyMs=response.metadata.latencyMs,
            timestamp=response.timestamp,
        )

        self.audit_records.append(record)
        self.metrics.totalProcessed += 1
        if response.status == "SUCCESS":
            self.metrics.successCount += 1
        else:
            self.metrics.failureCount += 1
        self.metrics.totalTokensUsed += response.usage.totalTokens
        self.metrics.totalLatencyMs += response.metadata.latencyMs

        logger.info(
            f"[{self.subscriber_id}] AUDIT LOG: eventId={record.eventId}, "
            f"correlationId={record.correlationId}, agent={record.agentId}, status={record.status}, "
            f"tokens={record.totalTokens}, latency={record.latencyMs}ms"
        )

        # Optionally publish to audit.logs topic
        try:
            producer = self._get_producer()
            producer.send(
                self.config.audit_logs_topic,
                key=record.correlationId,
                value=record.__dict__,
            )
        except Exception as e:
            logger.debug(f"Could not forward to audit.logs topic: {e}")

        return record

    def run(self, max_messages: Optional[int] = None) -> List[AuditRecord]:
        """Continuously consume and process agent response events."""
        consumer = self._get_consumer()
        self._running = True
        processed: List[AuditRecord] = []
        count = 0

        logger.info(f"[{self.subscriber_id}] Listening on topic '{self.config.agent_responses_topic}'")

        try:
            for message in consumer:
                if not self._running:
                    break
                try:
                    raw_data = message.value
                    if isinstance(raw_data, str):
                        raw_data = json.loads(raw_data)
                    response_event = AgentResponseEvent.model_validate(raw_data)
                    record = self.process_response(response_event)
                    processed.append(record)
                    count += 1
                    if max_messages and count >= max_messages:
                        break
                except Exception as ex:
                    logger.error(f"[{self.subscriber_id}] Error processing message: {ex}")
        finally:
            self.stop()

        return processed

    def stop(self):
        self._running = False
        if self._consumer is not None and hasattr(self._consumer, "close"):
            self._consumer.close()
            self._consumer = None
        if self._producer is not None and hasattr(self._producer, "close"):
            self._producer.close()
            self._producer = None
