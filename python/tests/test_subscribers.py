"""Unit tests for the 2 Event Subscribers."""

from unittest.mock import MagicMock
from python.src.config import PipelineConfig
from python.src.models import ActionItem, AgentResponseEvent, TaskType
from python.src.subscriber_action import ActionNotificationSubscriber
from python.src.subscriber_audit import AuditLogSubscriber


def test_audit_log_subscriber():
    mock_producer = MagicMock()
    config = PipelineConfig(audit_logs_topic="test.audit.logs")
    audit_sub = AuditLogSubscriber(config=config, kafka_producer=mock_producer)

    response_event = AgentResponseEvent.create(
        correlation_id="corr-999",
        task_type=TaskType.RESEARCH.value,
        response="Research completed successfully.",
        model="claude-3-5-sonnet-20241022",
        agent_id="claude-agent",
        prompt_tokens=100,
        completion_tokens=50,
        latency_ms=120,
    )

    record = audit_sub.process_response(response_event)

    assert record.correlationId == "corr-999"
    assert record.agentId == "claude-agent"
    assert record.totalTokens == 150
    assert record.latencyMs == 120
    assert audit_sub.metrics.totalProcessed == 1
    assert audit_sub.metrics.successCount == 1
    assert audit_sub.metrics.totalTokensUsed == 150
    assert audit_sub.metrics.averageLatencyMs == 120.0

    mock_producer.send.assert_called_once()
    args, kwargs = mock_producer.send.call_args
    assert args[0] == "test.audit.logs"


def test_action_notification_subscriber():
    mock_producer = MagicMock()
    config = PipelineConfig(task_notifications_topic="test.task.notifications")
    action_sub = ActionNotificationSubscriber(config=config, kafka_producer=mock_producer)

    actions = [
        ActionItem(actionType="NOTIFY_STAKEHOLDER", target="alerts-channel", parameters={"priority": "HIGH"}),
        ActionItem(actionType="POST_REVIEW_COMMENT", target="PR-101", parameters={"comment": "Approved"}),
    ]

    response_event = AgentResponseEvent.create(
        correlation_id="corr-888",
        task_type=TaskType.ACTION_PLANNING.value,
        response="Plan executed",
        model="claude-3-5-sonnet-20241022",
        action_required=True,
        actions=actions,
    )

    executed = action_sub.process_response(response_event)

    assert len(executed) == 2
    assert executed[0].actionType == "NOTIFY_STAKEHOLDER"
    assert executed[0].target == "alerts-channel"
    assert executed[0].status == "COMPLETED"
    assert executed[1].actionType == "POST_REVIEW_COMMENT"
    assert executed[1].target == "PR-101"

    assert mock_producer.send.call_count == 2
