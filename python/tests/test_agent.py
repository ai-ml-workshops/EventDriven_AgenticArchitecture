"""Unit tests for EventDrivenAgent."""

from unittest.mock import MagicMock
from python.src.agent import EventDrivenAgent
from python.src.config import PipelineConfig
from python.src.llm_client import MockClaudeLlmClient
from python.src.models import TaskRequestEvent, TaskType


def test_agent_process_event_success():
    mock_kafka_producer = MagicMock()
    config = PipelineConfig(agent_responses_topic="test.agent.responses")
    llm_client = MockClaudeLlmClient()

    agent = EventDrivenAgent(
        config=config,
        llm_client=llm_client,
        kafka_producer=mock_kafka_producer,
        agent_id="test-claude-agent",
    )

    request_event = TaskRequestEvent.create(
        prompt="Please conduct a code review of PR #5",
        task_type=TaskType.CODE_REVIEW,
        user_id="alice",
        context={"pr_id": "PR-5"},
    )

    response_event = agent.process_event(request_event)

    assert response_event.correlationId == request_event.correlationId
    assert response_event.agentId == "test-claude-agent"
    assert response_event.status == "SUCCESS"
    assert "CODE_REVIEW" in response_event.response
    assert response_event.usage.totalTokens > 0
    assert response_event.metadata.latencyMs >= 0

    mock_kafka_producer.send.assert_called_once()
    args, kwargs = mock_kafka_producer.send.call_args
    assert args[0] == "test.agent.responses"
    assert kwargs["key"] == request_event.correlationId


def test_agent_action_extraction():
    mock_kafka_producer = MagicMock()
    agent = EventDrivenAgent(
        config=PipelineConfig(),
        llm_client=MockClaudeLlmClient(),
        kafka_producer=mock_kafka_producer,
    )

    request_event = TaskRequestEvent.create(
        prompt="Create an action plan to migrate services",
        task_type=TaskType.ACTION_PLANNING,
        context={"notify_channel": "ops-channel"},
    )

    response = agent.process_event(request_event)
    assert response.metadata.actionRequired is True
    assert len(response.metadata.actions) >= 1
    assert response.metadata.actions[0].target == "ops-channel"
