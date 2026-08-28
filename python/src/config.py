"""Configuration settings for Kafka and LLM services."""

import os
from dataclasses import dataclass, field


@dataclass
class PipelineConfig:
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    task_requests_topic: str = field(
        default_factory=lambda: os.getenv("TASK_REQUESTS_TOPIC", "task.requests")
    )
    agent_responses_topic: str = field(
        default_factory=lambda: os.getenv("AGENT_RESPONSES_TOPIC", "agent.responses")
    )
    task_notifications_topic: str = field(
        default_factory=lambda: os.getenv("TASK_NOTIFICATIONS_TOPIC", "task.notifications")
    )
    audit_logs_topic: str = field(
        default_factory=lambda: os.getenv("AUDIT_LOGS_TOPIC", "audit.logs")
    )
    agent_consumer_group: str = field(
        default_factory=lambda: os.getenv("AGENT_CONSUMER_GROUP", "agentic-pipeline-agent-group")
    )
    audit_consumer_group: str = field(
        default_factory=lambda: os.getenv("AUDIT_CONSUMER_GROUP", "agentic-pipeline-audit-group")
    )
    action_consumer_group: str = field(
        default_factory=lambda: os.getenv("ACTION_CONSUMER_GROUP", "agentic-pipeline-action-group")
    )
    anthropic_api_key: str = field(
        default_factory=lambda: os.getenv("ANTHROPIC_API_KEY", "")
    )
    llm_model: str = field(
        default_factory=lambda: os.getenv("LLM_MODEL", "claude-3-5-sonnet-20241022")
    )
    use_mock_llm: bool = field(
        default_factory=lambda: os.getenv("USE_MOCK_LLM", "false").lower() in ("true", "1", "yes")
    )
    max_tokens: int = field(
        default_factory=lambda: int(os.getenv("MAX_TOKENS", "1024"))
    )
    temperature: float = field(
        default_factory=lambda: float(os.getenv("TEMPERATURE", "0.7"))
    )
