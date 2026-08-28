package com.aiml.eventdriven.llm;

import com.aiml.eventdriven.config.PipelineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlmClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(LlmClientFactory.class);

    public static LlmClient create(PipelineConfig config) {
        if (config.isUseMockLlm() || config.getAnthropicApiKey() == null || config.getAnthropicApiKey().trim().isEmpty()) {
            logger.info("Using Mock Claude LLM Client (no ANTHROPIC_API_KEY provided or USE_MOCK_LLM=true)");
            return new MockClaudeLlmClient(config.getLlmModel() + "-mock");
        } else {
            logger.info("Using Anthropic Claude LLM Client with model {}", config.getLlmModel());
            return new ClaudeLlmClient(
                    config.getAnthropicApiKey(),
                    config.getLlmModel(),
                    config.getMaxTokens(),
                    config.getTemperature()
            );
        }
    }
}
