"""Unit tests for EventProducer."""

from unittest.mock import MagicMock
from python.src.config import PipelineConfig
from python.src.models import TaskRequestEvent, TaskType
from python.src.producer import EventProducer


def test_producer_send_event():
    mock_kafka_producer = MagicMock()
    config = PipelineConfig(task_requests_topic="test.task.requests")
    producer = EventProducer(config=config, kafka_producer=mock_kafka_producer)

    event = TaskRequestEvent.create(
        prompt="Execute task 1",
        task_type=TaskType.RESEARCH,
        user_id="researcher-1",
    )

    producer.send_event(event)

    mock_kafka_producer.send.assert_called_once()
    args, kwargs = mock_kafka_producer.send.call_args
    assert args[0] == "test.task.requests"
    assert kwargs["key"] == event.correlationId
    assert kwargs["value"]["payload"]["prompt"] == "Execute task 1"


def test_producer_create_and_send_task():
    mock_kafka_producer = MagicMock()
    config = PipelineConfig(task_requests_topic="test.task.requests")
    producer = EventProducer(config=config, kafka_producer=mock_kafka_producer)

    event = producer.create_and_send_task(
        prompt="Review pull request #10",
        task_type=TaskType.CODE_REVIEW,
        user_id="dev-user",
        context={"pr_id": 10},
    )

    assert event.payload.prompt == "Review pull request #10"
    assert event.taskType == "CODE_REVIEW"
    mock_kafka_producer.send.assert_called_once()
