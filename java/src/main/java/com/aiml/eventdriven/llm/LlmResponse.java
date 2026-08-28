package com.aiml.eventdriven.llm;

public class LlmResponse {
    private final String content;
    private final String model;
    private final int promptTokens;
    private final int completionTokens;
    private final long latencyMs;

    public LlmResponse(String content, String model, int promptTokens, int completionTokens, long latencyMs) {
        this.content = content;
        this.model = model;
        this.promptTokens = promptTokens;
        this.completionTokens = completionTokens;
        this.latencyMs = latencyMs;
    }

    public String getContent() { return content; }
    public String getModel() { return model; }
    public int getPromptTokens() { return promptTokens; }
    public int getCompletionTokens() { return completionTokens; }
    public long getLatencyMs() { return latencyMs; }
    public int getTotalTokens() { return promptTokens + completionTokens; }
}
