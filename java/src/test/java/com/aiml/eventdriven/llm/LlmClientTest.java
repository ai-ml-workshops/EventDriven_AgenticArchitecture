package com.aiml.eventdriven.llm;

import com.aiml.eventdriven.config.PipelineConfig;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class LlmClientTest {

    @Test
    public void testMockClaudeLlmClient() {
        MockClaudeLlmClient client = new MockClaudeLlmClient();

        LlmResponse codeReviewResp = client.generate("Please perform a code review on PR #1", null, null, null);
        assertTrue(codeReviewResp.getContent().contains("CODE_REVIEW"));
        assertTrue(codeReviewResp.getPromptTokens() > 0);
        assertTrue(codeReviewResp.getCompletionTokens() > 0);

        LlmResponse researchResp = client.generate("Research Kafka topics", null, null, null);
        assertTrue(researchResp.getContent().contains("RESEARCH"));

        LlmResponse generalResp = client.generate("General question", null, null, null);
        assertTrue(generalResp.getContent().contains("General question"));
    }

    @Test
    public void testLlmClientFactory() {
        PipelineConfig configMock = new PipelineConfig();
        configMock.setAnthropicApiKey("");
        configMock.setUseMockLlm(false);

        LlmClient client = LlmClientFactory.create(configMock);
        assertTrue(client instanceof MockClaudeLlmClient);

        PipelineConfig configExplicitMock = new PipelineConfig();
        configExplicitMock.setAnthropicApiKey("sk-test");
        configExplicitMock.setUseMockLlm(true);

        LlmClient client2 = LlmClientFactory.create(configExplicitMock);
        assertTrue(client2 instanceof MockClaudeLlmClient);

        PipelineConfig configReal = new PipelineConfig();
        configReal.setAnthropicApiKey("sk-test");
        configReal.setUseMockLlm(false);

        LlmClient client3 = LlmClientFactory.create(configReal);
        assertTrue(client3 instanceof ClaudeLlmClient);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testClaudeLlmClientWithMockHttpClient() throws Exception {
        HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
        HttpResponse<String> mockResponse = Mockito.mock(HttpResponse.class);

        String jsonResponseBody = "{\n" +
                "  \"id\": \"msg_01\",\n" +
                "  \"type\": \"message\",\n" +
                "  \"role\": \"assistant\",\n" +
                "  \"content\": [\n" +
                "    {\"type\": \"text\", \"text\": \"Claude generated architectural analysis.\"}\n" +
                "  ],\n" +
                "  \"model\": \"claude-3-5-sonnet-20241022\",\n" +
                "  \"usage\": {\n" +
                "    \"input_tokens\": 35,\n" +
                "    \"output_tokens\": 20\n" +
                "  }\n" +
                "}";

        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(jsonResponseBody);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockResponse);

        ClaudeLlmClient client = new ClaudeLlmClient("test-key", "claude-3-5-sonnet-20241022", 1024, 0.7, mockHttpClient);

        LlmResponse response = client.generate("Explain event streaming", "You are an architect", Map.of("framework", "Kafka"), null);

        assertEquals("Claude generated architectural analysis.", response.getContent());
        assertEquals("claude-3-5-sonnet-20241022", response.getModel());
        assertEquals(35, response.getPromptTokens());
        assertEquals(20, response.getCompletionTokens());
        assertEquals(55, response.getTotalTokens());
        assertTrue(response.getLatencyMs() >= 0);
    }
}
