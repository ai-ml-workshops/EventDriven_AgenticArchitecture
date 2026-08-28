package com.aiml.eventdriven.llm;

import java.util.Map;

public class MockClaudeLlmClient implements LlmClient {
    private final String model;

    public MockClaudeLlmClient() {
        this("claude-3-5-sonnet-20241022-mock");
    }

    public MockClaudeLlmClient(String model) {
        this.model = model;
    }

    @Override
    public String getModelName() {
        return model;
    }

    @Override
    public LlmResponse generate(String prompt, String systemPrompt, Map<String, Object> context, Map<String, Object> parameters) {
        long startTime = System.currentTimeMillis();
        String promptLower = prompt.toLowerCase();
        String content;

        if (promptLower.contains("code review") || promptLower.contains("review")) {
            content = "[Claude Agent Analysis - CODE_REVIEW]\n"
                    + "Summary: Java agent code review completed.\n"
                    + "Findings:\n"
                    + "1. Immutability patterns applied to event payloads.\n"
                    + "2. Exception handling with structured fallback in place.\n"
                    + "Recommendation: Ready for deployment.";
        } else if (promptLower.contains("research")) {
            content = "[Claude Agent Analysis - RESEARCH]\n"
                    + "Summary: Research conducted on Kafka event streaming and agentic architectures.\n"
                    + "Key Insights:\n"
                    + "1. Kafka provides durable ordering and consumer group load balancing.\n"
                    + "2. Event-driven agents scale independently from publishers and subscribers.";
        } else if (promptLower.contains("action") || promptLower.contains("plan")) {
            content = "[Claude Agent Analysis - ACTION_PLANNING]\n"
                    + "Plan:\n"
                    + "1. Ingest task event.\n"
                    + "2. Execute LLM reasoning step.\n"
                    + "3. Publish response to downstream subscribers.";
        } else {
            content = "[Claude Agent Response]\n"
                    + "Processed prompt: '" + prompt + "'.\n"
                    + "Agent reasoning: Successfully completed task execution in Java agentic pipeline.";
        }

        int promptTokens = Math.max(prompt.split("\\s+").length * 2, 10);
        int completionTokens = Math.max(content.split("\\s+").length * 2, 25);
        long latencyMs = System.currentTimeMillis() - startTime + 15;

        return new LlmResponse(content, model, promptTokens, completionTokens, latencyMs);
    }
}
