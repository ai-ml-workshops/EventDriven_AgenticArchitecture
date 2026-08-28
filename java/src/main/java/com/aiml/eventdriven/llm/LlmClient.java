package com.aiml.eventdriven.llm;

import java.util.Map;

public interface LlmClient {
    LlmResponse generate(String prompt, String systemPrompt, Map<String, Object> context, Map<String, Object> parameters);
    String getModelName();
}
