"""Event-Driven Agent that consumes task requests, interacts with LLM (Claude), and emits response events."""

import json
import logging
import time
from typing import Any, Dict, List, Optional

from .config import PipelineConfig
from .llm_client import LlmClient, get_llm_client
from .models import (
    ActionItem,
    AgentResponseEvent,
    ResponseMetadata,
    TaskRequestEvent,
    TaskStatus,
    TaskType,
    TokenUsage,
)

logger = logging.getLogger(__name__)


class EventDrivenAgent:
    """Agent that processes TaskRequestEvents from Kafka using an LLM and outputs AgentResponseEvents."""

    def __init__(
        self,
        config: Optional[PipelineConfig] = None,
        llm_client: Optional[LlmClient] = None,
        kafka_consumer=None,
        kafka_producer=None,
        agent_id: str = "claude-workflow-agent-python",
    ):
        self.config = config or PipelineConfig()
        self.agent_id = agent_id
        self.llm_client = llm_client or get_llm_client(self.config)
        self._external_consumer = kafka_consumer
        self._external_producer = kafka_producer
        self._consumer = None
        self._producer = None
        self._running = False

    def _get_consumer(self):
        if self._external_consumer is not None:
            return self._external_consumer
        if self._consumer is None:
            from kafka import KafkaConsumer
            self._consumer = KafkaConsumer(
                self.config.task_requests_topic,
                bootstrap_servers=self.config.kafka_bootstrap_servers.split(","),
                group_id=self.config.agent_consumer_group,
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

    def _extract_actions(self, task_type: str, llm_output: str, payload_context: Dict[str, Any]) -> List[ActionItem]:
        """Determine if downstream actions are needed based on task type and LLM output."""
        actions: List[ActionItem] = []
        output_lower = llm_output.lower()

        if task_type in (TaskType.ACTION_PLANNING.value, "ACTION_PLANNING") or "action" in output_lower or "notify" in output_lower:
            actions.append(
                ActionItem(
                    actionType="NOTIFY_STAKEHOLDER",
                    target=payload_context.get("notify_channel", "general-alerts"),
                    parameters={"summary": "Agent completed task requiring action notification", "priority": "HIGH"},
                )
            )
        if task_type in (TaskType.CODE_REVIEW.value, "CODE_REVIEW") and "recommendation" in output_lower:
            actions.append(
                ActionItem(
                    actionType="POST_REVIEW_COMMENT",
                    target=payload_context.get("pr_id", "default-pr"),
                    parameters={"comment": "Automated code review findings ready."},
                )
            )
        return actions

    def process_event(self, event: TaskRequestEvent) -> AgentResponseEvent:
        """Processes a single TaskRequestEvent by invoking the LLM and constructing an AgentResponseEvent."""
        logger.info(f"Agent [{self.agent_id}] processing event {event.eventId} (taskType={event.taskType})")
        start_time = time.time()

        system_prompt = (
            f"You are an expert AI agent ({self.agent_id}) specialized in executing {event.taskType} workflows. "
            "Analyze the prompt and provided context carefully, reason through the problem, and provide a clear, structured solution."
        )

        try:
            llm_res = self.llm_client.generate(
                prompt=event.payload.prompt,
                system_prompt=system_prompt,
                context=event.payload.context,
                parameters=event.payload.parameters,
            )
            elapsed_ms = int((time.time() - start_time) * 1000)

            actions = self._extract_actions(event.taskType, llm_res.content, event.payload.context)
            action_required = len(actions) > 0

            response_event = AgentResponseEvent(
                correlationId=event.correlationId,
                agentId=self.agent_id,
                taskType=event.taskType,
                status=TaskStatus.SUCCESS.value,
                response=llm_res.content,
                model=llm_res.model,
                usage=TokenUsage(
                    promptTokens=llm_res.prompt_tokens,
                    completionTokens=llm_res.completion_tokens,
                    totalTokens=llm_res.prompt_tokens + llm_res.completion_tokens,
                ),
                metadata=ResponseMetadata(
                    latencyMs=elapsed_ms,
                    actionRequired=action_required,
                    actions=actions,
                ),
            )
        except Exception as e:
            elapsed_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Agent [{self.agent_id}] encountered error processing event {event.eventId}: {e}")
            response_event = AgentResponseEvent(
                correlationId=event.correlationId,
                agentId=self.agent_id,
                taskType=event.taskType,
                status=TaskStatus.FAILED.value,
                response=f"Error executing task: {str(e)}",
                model=getattr(self.llm_client, "model", "unknown"),
                usage=TokenUsage(promptTokens=0, completionTokens=0, totalTokens=0),
                metadata=ResponseMetadata(
                    latencyMs=elapsed_ms,
                    actionRequired=False,
                    actions=[],
                ),
            )

        # Publish response event to Kafka
        self._publish_response(response_event)
        return response_event

    def _publish_response(self, response_event: AgentResponseEvent):
        producer = self._get_producer()
        topic = self.config.agent_responses_topic
        key = response_event.correlationId
        value = response_event.model_dump()

        logger.info(
            f"Agent [{self.agent_id}] publishing response {response_event.eventId} "
            f"(correlationId={key}, status={response_event.status}) to topic '{topic}'"
        )
        producer.send(topic, key=key, value=value)
        if hasattr(producer, "flush"):
            producer.flush()

    def run(self, max_messages: Optional[int] = None) -> List[AgentResponseEvent]:
        """Continuously consume and process task request events."""
        consumer = self._get_consumer()
        self._running = True
        processed_events: List[AgentResponseEvent] = []
        count = 0

        logger.info(f"Agent [{self.agent_id}] started listening on topic '{self.config.task_requests_topic}'")

        try:
            for message in consumer:
                if not self._running:
                    break
                try:
                    raw_data = message.value
                    if isinstance(raw_data, str):
                        raw_data = json.loads(raw_data)
                    task_event = TaskRequestEvent.model_validate(raw_data)
                    response = self.process_event(task_event)
                    processed_events.append(response)
                    count += 1
                    if max_messages and count >= max_messages:
                        break
                except Exception as ex:
                    logger.error(f"Failed to process Kafka message: {ex}")
        finally:
            self.stop()

        return processed_events

    def stop(self):
        self._running = False
        if self._consumer is not None and hasattr(self._consumer, "close"):
            self._consumer.close()
            self._consumer = None
        if self._producer is not None and hasattr(self._producer, "close"):
            self._producer.close()
            self._producer = None
