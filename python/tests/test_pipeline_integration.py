"""End-to-End integration test simulating the full event flow in-memory."""

from python.src.agent import EventDrivenAgent
from python.src.config import PipelineConfig
from python.src.llm_client import MockClaudeLlmClient
from python.src.models import TaskRequestEvent, TaskType
from python.src.producer import EventProducer
from python.src.subscriber_action import ActionNotificationSubscriber
from python.src.subscriber_audit import AuditLogSubscriber


class InMemoryBroker:
    """Simple in-memory broker queue for integration testing without an external Kafka container."""

    def __init__(self):
        self.topics = {}

    def publish(self, topic: str, key: str, value: dict):
        if topic not in self.topics:
            self.topics[topic] = []
        self.topics[topic].append({"key": key, "value": value})

    def get_messages(self, topic: str):
        return self.topics.get(topic, [])


class MockKafkaProducerWrapper:
    def __init__(self, broker: InMemoryBroker):
        self.broker = broker

    def send(self, topic, key=None, value=None):
        self.broker.publish(topic, key, value)
        return self

    def flush(self):
        pass


def test_full_agentic_pipeline_flow():
    broker = InMemoryBroker()
    producer_wrapper = MockKafkaProducerWrapper(broker)

    config = PipelineConfig(
        task_requests_topic="task.requests",
        agent_responses_topic="agent.responses",
        audit_logs_topic="audit.logs",
        task_notifications_topic="task.notifications",
    )

    # 1. Initialize Producer
    producer = EventProducer(config=config, kafka_producer=producer_wrapper)

    # 2. Initialize Agent
    llm_client = MockClaudeLlmClient(model="claude-3-5-sonnet-20241022-mock")
    agent = EventDrivenAgent(
        config=config,
        llm_client=llm_client,
        kafka_producer=producer_wrapper,
        agent_id="claude-agent-integration",
    )

    # 3. Initialize 2 Subscribers
    audit_sub = AuditLogSubscriber(config=config, kafka_producer=producer_wrapper)
    action_sub = ActionNotificationSubscriber(config=config, kafka_producer=producer_wrapper)

    # 4. Producer emits 3 different task events
    tasks = [
        ("Review security middleware", TaskType.CODE_REVIEW, {"pr_id": "PR-201"}),
        ("Research distributed commit logs", TaskType.RESEARCH, {"topic_id": "RES-42"}),
        ("Plan migration steps", TaskType.ACTION_PLANNING, {"notify_channel": "dev-ops"}),
    ]

    sent_events = []
    for prompt, task_type, ctx in tasks:
        event = producer.create_and_send_task(
            prompt=prompt,
            task_type=task_type,
            user_id="integration-tester",
            context=ctx,
        )
        sent_events.append(event)

    # Check that events reached the task.requests topic
    req_messages = broker.get_messages("task.requests")
    assert len(req_messages) == 3

    # 5. Agent processes each task event from topic
    for msg in req_messages:
        req_event = TaskRequestEvent.model_validate(msg["value"])
        agent.process_event(req_event)

    # Check that agent responses reached the agent.responses topic
    resp_messages = broker.get_messages("agent.responses")
    assert len(resp_messages) == 3

    # 6. Both Subscribers consume each agent response event
    for msg in resp_messages:
        resp_event = msg["value"]
        from python.src.models import AgentResponseEvent
        response_model = AgentResponseEvent.model_validate(resp_event)

        # Subscriber 1: Audit
        audit_sub.process_response(response_model)

        # Subscriber 2: Action & Notification
        action_sub.process_response(response_model)

    # 7. Assertions on Subscriber 1 (Audit)
    assert audit_sub.metrics.totalProcessed == 3
    assert audit_sub.metrics.successCount == 3
    assert audit_sub.metrics.totalTokensUsed > 0
    assert len(audit_sub.audit_records) == 3
    assert broker.get_messages("audit.logs") is not None

    # 8. Assertions on Subscriber 2 (Action Notifications)
    # The CODE_REVIEW and ACTION_PLANNING tasks should trigger actions
    assert len(action_sub.executed_actions) >= 2
    executed_types = [a.actionType for a in action_sub.executed_actions]
    assert "NOTIFY_STAKEHOLDER" in executed_types
    assert len(broker.get_messages("task.notifications")) >= 2
