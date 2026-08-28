"""Unit tests for event data models and schemas."""

import json
from python.src.models import (
    ActionItem,
    AgentResponseEvent,
    ResponseMetadata,
    TaskPayload,
    TaskRequestEvent,
    TaskStatus,
    TaskType,
    TokenUsage,
)


def test_task_request_event_creation():
    event = TaskRequestEvent.create(
        prompt="Review pull request #42",
        task_type=TaskType.CODE_REVIEW,
        user_id="alice",
        context={"repo": "my-org/my-repo"},
        parameters={"temperature": 0.2},
    )

    assert event.eventId is not None
    assert event.correlationId is not None
    assert event.taskType == "CODE_REVIEW"
    assert event.userId == "alice"
    assert event.payload.prompt == "Review pull request #42"
    assert event.payload.context["repo"] == "my-org/my-repo"
    assert event.schemaVersion == "1.0.0"

    # Test JSON serialization & deserialization
    json_str = event.to_json()
    deserialized = TaskRequestEvent.from_json(json_str)
    assert deserialized.eventId == event.eventId
    assert deserialized.correlationId == event.correlationId
    assert deserialized.payload.prompt == event.payload.prompt


def test_agent_response_event_creation():
    actions = [
        ActionItem(actionType="NOTIFY_STAKEHOLDER", target="slack-channel", parameters={"msg": "Done"})
    ]
    response = AgentResponseEvent.create(
        correlation_id="corr-12345",
        task_type=TaskType.CODE_REVIEW.value,
        response="Code is clean and tests pass.",
        model="claude-3-5-sonnet-20241022",
        agent_id="test-agent",
        prompt_tokens=150,
        completion_tokens=50,
        latency_ms=230,
        action_required=True,
        actions=actions,
    )

    assert response.correlationId == "corr-12345"
    assert response.agentId == "test-agent"
    assert response.status == TaskStatus.SUCCESS.value
    assert response.usage.promptTokens == 150
    assert response.usage.completionTokens == 50
    assert response.usage.totalTokens == 200
    assert response.metadata.latencyMs == 230
    assert response.metadata.actionRequired is True
    assert len(response.metadata.actions) == 1
    assert response.metadata.actions[0].actionType == "NOTIFY_STAKEHOLDER"

    # Test JSON serialization & deserialization
    json_str = response.to_json()
    deserialized = AgentResponseEvent.from_json(json_str)
    assert deserialized.eventId == response.eventId
    assert deserialized.correlationId == response.correlationId
    assert deserialized.usage.totalTokens == 200
    assert deserialized.metadata.actions[0].target == "slack-channel"
