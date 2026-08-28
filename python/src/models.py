"""Data models and event schemas for the event-driven agentic pipeline."""

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class TaskType(str, Enum):
    CODE_REVIEW = "CODE_REVIEW"
    RESEARCH = "RESEARCH"
    TEXT_ANALYSIS = "TEXT_ANALYSIS"
    ACTION_PLANNING = "ACTION_PLANNING"
    GENERAL_ASSISTANT = "GENERAL_ASSISTANT"


class TaskStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TaskPayload(BaseModel):
    prompt: str = Field(..., description="The main prompt/instruction for the agent")
    context: Dict[str, Any] = Field(default_factory=dict, description="Contextual information")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Execution parameters")


class TaskRequestEvent(BaseModel):
    eventId: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlationId: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    taskType: str = Field(default=TaskType.GENERAL_ASSISTANT.value)
    userId: str = Field(default="system-user")
    payload: TaskPayload
    schemaVersion: str = Field(default="1.0.0")

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "TaskRequestEvent":
        return cls.model_validate_json(json_str)

    @classmethod
    def create(
        cls,
        prompt: str,
        task_type: TaskType = TaskType.GENERAL_ASSISTANT,
        user_id: str = "default-user",
        context: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
    ) -> "TaskRequestEvent":
        return cls(
            correlationId=correlation_id or str(uuid.uuid4()),
            taskType=task_type.value if isinstance(task_type, TaskType) else str(task_type),
            userId=user_id,
            payload=TaskPayload(
                prompt=prompt,
                context=context or {},
                parameters=parameters or {},
            ),
        )


class TokenUsage(BaseModel):
    promptTokens: int = Field(default=0, ge=0)
    completionTokens: int = Field(default=0, ge=0)
    totalTokens: int = Field(default=0, ge=0)


class ActionItem(BaseModel):
    actionType: str
    target: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


class ResponseMetadata(BaseModel):
    latencyMs: int = Field(default=0, ge=0)
    actionRequired: bool = Field(default=False)
    actions: List[ActionItem] = Field(default_factory=list)


class AgentResponseEvent(BaseModel):
    eventId: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlationId: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    agentId: str = Field(default="claude-agent")
    taskType: str
    status: str = Field(default=TaskStatus.SUCCESS.value)
    response: str
    model: str
    usage: TokenUsage = Field(default_factory=TokenUsage)
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    schemaVersion: str = Field(default="1.0.0")

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "AgentResponseEvent":
        return cls.model_validate_json(json_str)

    @classmethod
    def create(
        cls,
        correlation_id: str,
        task_type: str,
        response: str,
        model: str,
        agent_id: str = "claude-agent-python",
        status: TaskStatus = TaskStatus.SUCCESS,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        latency_ms: int = 0,
        action_required: bool = False,
        actions: Optional[List[ActionItem]] = None,
    ) -> "AgentResponseEvent":
        return cls(
            correlationId=correlation_id,
            agentId=agent_id,
            taskType=task_type,
            status=status.value if isinstance(status, TaskStatus) else str(status),
            response=response,
            model=model,
            usage=TokenUsage(
                promptTokens=prompt_tokens,
                completionTokens=completion_tokens,
                totalTokens=prompt_tokens + completion_tokens,
            ),
            metadata=ResponseMetadata(
                latencyMs=latency_ms,
                actionRequired=action_required,
                actions=actions or [],
            ),
        )
