"""LLM Client interface and implementations for interacting with models like Claude."""

import abc
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .config import PipelineConfig

logger = logging.getLogger(__name__)


@dataclass
class LlmResponse:
    content: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    latency_ms: int


class LlmClient(abc.ABC):
    """Abstract interface for LLM clients."""

    @abc.abstractmethod
    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> LlmResponse:
        """Generate a response from the LLM."""
        pass


class ClaudeLlmClient(LlmClient):
    """Client for interacting with Anthropic's Claude API."""

    def __init__(self, api_key: str, model: str = "claude-3-5-sonnet-20241022", max_tokens: int = 1024, temperature: float = 0.7):
        self.api_key = api_key
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                raise ImportError("anthropic package is required for ClaudeLlmClient. Install with: pip install anthropic")
        return self._client

    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> LlmResponse:
        client = self._get_client()
        params = parameters or {}
        max_tokens = params.get("max_tokens", self.max_tokens)
        temperature = params.get("temperature", self.temperature)
        model = params.get("model", self.model)

        # Build message context
        user_content = prompt
        if context:
            context_str = json.dumps(context, indent=2)
            user_content = f"Context:\n{context_str}\n\nTask:\n{prompt}"

        system_msg = system_prompt or "You are an intelligent agent executing workflows in an event-driven system."

        start_time = time.time()
        try:
            message = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system_msg,
                messages=[
                    {"role": "user", "content": user_content}
                ],
            )
            elapsed_ms = int((time.time() - start_time) * 1000)

            content_text = ""
            if message.content:
                for block in message.content:
                    if hasattr(block, "text"):
                        content_text += block.text

            prompt_tokens = message.usage.input_tokens if hasattr(message, "usage") and message.usage else 0
            completion_tokens = message.usage.output_tokens if hasattr(message, "usage") and message.usage else 0

            return LlmResponse(
                content=content_text,
                model=model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                latency_ms=elapsed_ms,
            )
        except Exception as e:
            logger.error(f"Error calling Claude API: {e}")
            raise


class MockClaudeLlmClient(LlmClient):
    """Mock Claude LLM client for testing and offline local execution."""

    def __init__(self, model: str = "claude-3-5-sonnet-20241022-mock"):
        self.model = model

    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> LlmResponse:
        start_time = time.time()
        
        prompt_lower = prompt.lower()
        if "code review" in prompt_lower or "review" in prompt_lower:
            content = (
                "[Claude Agent Analysis - CODE_REVIEW]\n"
                "Summary: Code review completed. Identified standard formatting and exception handling patterns.\n"
                "Findings:\n"
                "1. Input validation is present.\n"
                "2. Error logging follows structured JSON format.\n"
                "Recommendation: Approved with minor suggestions for test coverage."
            )
        elif "research" in prompt_lower:
            content = (
                "[Claude Agent Analysis - RESEARCH]\n"
                "Summary: Research topic analyzed across relevant architectural patterns.\n"
                "Key Insights:\n"
                "1. Event-driven architectures decouple producers, agents, and consumers.\n"
                "2. Kafka provides durable commit-log messaging with partitioned topic parallelism."
            )
        elif "action" in prompt_lower or "plan" in prompt_lower:
            content = (
                "[Claude Agent Analysis - ACTION_PLANNING]\n"
                "Plan:\n"
                "1. Ingest input task event.\n"
                "2. Dispatch notification to subscribers.\n"
                "3. Persist audit trail."
            )
        else:
            content = (
                f"[Claude Agent Response]\n"
                f"Processed prompt: '{prompt}'.\n"
                f"Agent reasoning: Successfully evaluated the event payload and generated the workflow response."
            )

        # Estimate mock tokens
        prompt_tokens = max(len(prompt.split()) * 2, 10)
        completion_tokens = max(len(content.split()) * 2, 25)
        elapsed_ms = int((time.time() - start_time) * 1000) + 15

        return LlmResponse(
            content=content,
            model=self.model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            latency_ms=elapsed_ms,
        )


def get_llm_client(config: PipelineConfig) -> LlmClient:
    """Factory to instantiate the appropriate LLM client based on configuration."""
    if config.use_mock_llm or not config.anthropic_api_key:
        logger.info("Using Mock Claude LLM Client (no ANTHROPIC_API_KEY provided or USE_MOCK_LLM=true)")
        return MockClaudeLlmClient(model=f"{config.llm_model}-mock")
    else:
        logger.info(f"Using Anthropic Claude LLM Client with model {config.llm_model}")
        return ClaudeLlmClient(
            api_key=config.anthropic_api_key,
            model=config.llm_model,
            max_tokens=config.max_tokens,
            temperature=config.temperature,
        )
