"""Unit tests for LLM client implementations."""

from unittest.mock import MagicMock, patch
from python.src.config import PipelineConfig
from python.src.llm_client import ClaudeLlmClient, MockClaudeLlmClient, get_llm_client


def test_mock_claude_llm_client():
    mock_client = MockClaudeLlmClient()
    
    # Code review prompt
    resp_code = mock_client.generate("Please perform a code review on the pull request.")
    assert "CODE_REVIEW" in resp_code.content
    assert resp_code.prompt_tokens > 0
    assert resp_code.completion_tokens > 0

    # Research prompt
    resp_res = mock_client.generate("Research Kafka partitioned topics.")
    assert "RESEARCH" in resp_res.content

    # General prompt
    resp_gen = mock_client.generate("Hello world")
    assert "Hello world" in resp_gen.content


def test_get_llm_client_factory():
    # Without API key -> mock client
    config_mock = PipelineConfig(anthropic_api_key="", use_mock_llm=False)
    client = get_llm_client(config_mock)
    assert isinstance(client, MockClaudeLlmClient)

    # Explicit use_mock_llm=True
    config_explicit_mock = PipelineConfig(anthropic_api_key="dummy-key", use_mock_llm=True)
    client2 = get_llm_client(config_explicit_mock)
    assert isinstance(client2, MockClaudeLlmClient)

    # With API key -> Claude client
    config_real = PipelineConfig(anthropic_api_key="sk-ant-test-key", use_mock_llm=False)
    client3 = get_llm_client(config_real)
    assert isinstance(client3, ClaudeLlmClient)
    assert client3.api_key == "sk-ant-test-key"


def test_claude_llm_client_with_mocked_anthropic():
    client = ClaudeLlmClient(api_key="test-key", model="claude-3-5-sonnet-20241022")
    
    mock_anthropic = MagicMock()
    mock_message = MagicMock()
    mock_block = MagicMock()
    mock_block.text = "Claude response text from API"
    mock_message.content = [mock_block]
    mock_message.usage.input_tokens = 42
    mock_message.usage.output_tokens = 18
    mock_anthropic.messages.create.return_value = mock_message

    client._client = mock_anthropic

    response = client.generate(
        prompt="Explain Kafka KRaft mode",
        system_prompt="You are an architect",
        context={"version": "3.7.0"},
    )

    assert response.content == "Claude response text from API"
    assert response.model == "claude-3-5-sonnet-20241022"
    assert response.prompt_tokens == 42
    assert response.completion_tokens == 18
    assert response.latency_ms >= 0

    mock_anthropic.messages.create.assert_called_once()
