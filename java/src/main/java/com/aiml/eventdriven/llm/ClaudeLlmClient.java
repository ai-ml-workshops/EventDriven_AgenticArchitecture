package com.aiml.eventdriven.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClaudeLlmClient implements LlmClient {
    private static final Logger logger = LoggerFactory.getLogger(ClaudeLlmClient.class);
    private static final String ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
    private static final String ANTHROPIC_VERSION = "2023-06-01";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String apiKey;
    private final String model;
    private final int maxTokens;
    private final double temperature;
    private final HttpClient httpClient;

    public ClaudeLlmClient(String apiKey, String model, int maxTokens, double temperature) {
        this(apiKey, model, maxTokens, temperature, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build());
    }

    public ClaudeLlmClient(String apiKey, String model, int maxTokens, double temperature, HttpClient httpClient) {
        this.apiKey = apiKey;
        this.model = model != null ? model : "claude-3-5-sonnet-20241022";
        this.maxTokens = maxTokens > 0 ? maxTokens : 1024;
        this.temperature = temperature;
        this.httpClient = httpClient;
    }

    @Override
    public String getModelName() {
        return model;
    }

    @Override
    public LlmResponse generate(String prompt, String systemPrompt, Map<String, Object> context, Map<String, Object> parameters) {
        long startTime = System.currentTimeMillis();

        String userContent = prompt;
        if (context != null && !context.isEmpty()) {
            try {
                String ctxStr = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(context);
                userContent = "Context:\n" + ctxStr + "\n\nTask:\n" + prompt;
            } catch (Exception e) {
                userContent = "Context: " + context + "\n\nTask: " + prompt;
            }
        }

        String sysMsg = (systemPrompt != null && !systemPrompt.trim().isEmpty())
                ? systemPrompt
                : "You are an intelligent agent executing workflows in an event-driven system.";

        int effectiveMaxTokens = maxTokens;
        double effectiveTemp = temperature;
        String effectiveModel = model;

        if (parameters != null) {
            if (parameters.containsKey("max_tokens")) {
                effectiveMaxTokens = ((Number) parameters.get("max_tokens")).intValue();
            }
            if (parameters.containsKey("temperature")) {
                effectiveTemp = ((Number) parameters.get("temperature")).doubleValue();
            }
            if (parameters.containsKey("model")) {
                effectiveModel = (String) parameters.get("model");
            }
        }

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", effectiveModel);
        requestBody.put("max_tokens", effectiveMaxTokens);
        requestBody.put("temperature", effectiveTemp);
        requestBody.put("system", sysMsg);
        requestBody.put("messages", List.of(Map.of("role", "user", "content", userContent)));

        try {
            String jsonPayload = MAPPER.writeValueAsString(requestBody);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ANTHROPIC_API_URL))
                    .header("x-api-key", apiKey)
                    .header("anthropic-version", ANTHROPIC_VERSION)
                    .header("content-type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            long latencyMs = System.currentTimeMillis() - startTime;

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                JsonNode root = MAPPER.readTree(response.body());
                StringBuilder contentBuilder = new StringBuilder();
                JsonNode contentNode = root.get("content");
                if (contentNode != null && contentNode.isArray()) {
                    for (JsonNode block : contentNode) {
                        if (block.has("text")) {
                            contentBuilder.append(block.get("text").asText());
                        }
                    }
                }

                int inputTokens = 0;
                int outputTokens = 0;
                JsonNode usageNode = root.get("usage");
                if (usageNode != null) {
                    if (usageNode.has("input_tokens")) {
                        inputTokens = usageNode.get("input_tokens").asInt();
                    }
                    if (usageNode.has("output_tokens")) {
                        outputTokens = usageNode.get("output_tokens").asInt();
                    }
                }

                return new LlmResponse(contentBuilder.toString(), effectiveModel, inputTokens, outputTokens, latencyMs);
            } else {
                logger.error("Claude API returned status {}: {}", response.statusCode(), response.body());
                throw new RuntimeException("Claude API error " + response.statusCode() + ": " + response.body());
            }
        } catch (Exception e) {
            logger.error("Error communicating with Claude API", e);
            throw new RuntimeException("Error communicating with Claude API: " + e.getMessage(), e);
        }
    }
}
