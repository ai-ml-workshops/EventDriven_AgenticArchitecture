"""Subscriber 2: Action and Notification Subscriber."""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .config import PipelineConfig
from .models import ActionItem, AgentResponseEvent

logger = logging.getLogger(__name__)


@dataclass
class ExecutedAction:
    actionType: str
    target: Optional[str]
    correlationId: str
    status: str
    details: Dict[str, Any] = field(default_factory=dict)


class ActionNotificationSubscriber:
    """Subscriber that executes actions and dispatches notifications based on agent responses."""

    def __init__(
        self,
        config: Optional[PipelineConfig] = None,
        kafka_consumer=None,
        kafka_producer=None,
        subscriber_id: str = "action-notification-subscriber-python",
    ):
        self.config = config or PipelineConfig()
        self.subscriber_id = subscriber_id
        self._external_consumer = kafka_consumer
        self._external_producer = kafka_producer
        self._consumer = None
        self._producer = None
        self._running = False
        self.executed_actions: List[ExecutedAction] = []

    def _get_consumer(self):
        if self._external_consumer is not None:
            return self._external_consumer
        if self._consumer is None:
            from kafka import KafkaConsumer
            self._consumer = KafkaConsumer(
                self.config.agent_responses_topic,
                bootstrap_servers=self.config.kafka_bootstrap_servers.split(","),
                group_id=self.config.action_consumer_group,
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

    def _execute_action(self, action: ActionItem, response: AgentResponseEvent) -> ExecutedAction:
        """Simulates/executes action side-effect (e.g. sending alert, triggering downstream API)."""
        logger.info(
            f"[{self.subscriber_id}] Executing action '{action.actionType}' "
            f"for correlationId={response.correlationId}, target='{action.target}'"
        )
        exec_result = ExecutedAction(
            actionType=action.actionType,
            target=action.target,
            correlationId=response.correlationId,
            status="COMPLETED",
            details={
                "taskType": response.taskType,
                "agentId": response.agentId,
                "parameters": action.parameters,
            },
        )
        self.executed_actions.append(exec_result)

        # Publish notification event to task.notifications topic
        try:
            producer = self._get_producer()
            notification_payload = {
                "correlationId": response.correlationId,
                "actionType": action.actionType,
                "target": action.target,
                "status": "COMPLETED",
                "summary": f"Agent {response.agentId} completed action {action.actionType}",
            }
            producer.send(
                self.config.task_notifications_topic,
                key=response.correlationId,
                value=notification_payload,
            )
        except Exception as e:
            logger.debug(f"Could not forward to task.notifications topic: {e}")

        return exec_result

    def process_response(self, response: AgentResponseEvent) -> List[ExecutedAction]:
        """Processes an AgentResponseEvent, checks for required actions, and executes them."""
        results: List[ExecutedAction] = []
        if response.metadata.actionRequired and response.metadata.actions:
            for action in response.metadata.actions:
                exec_action = self._execute_action(action, response)
                results.append(exec_action)
        else:
            logger.info(
                f"[{self.subscriber_id}] No action required for response {response.eventId} (correlationId={response.correlationId})"
            )
        return results

    def run(self, max_messages: Optional[int] = None) -> List[ExecutedAction]:
        """Continuously consume and process agent response events."""
        consumer = self._get_consumer()
        self._running = True
        all_actions: List[ExecutedAction] = []
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
                    actions = self.process_response(response_event)
                    all_actions.extend(actions)
                    count += 1
                    if max_messages and count >= max_messages:
                        break
                except Exception as ex:
                    logger.error(f"[{self.subscriber_id}] Error processing message: {ex}")
        finally:
            self.stop()

        return all_actions

    def stop(self):
        self._running = False
        if self._consumer is not None and hasattr(self._consumer, "close"):
            self._consumer.close()
            self._consumer = None
        if self._producer is not None and hasattr(self._producer, "close"):
            self._producer.close()
            self._producer = None
