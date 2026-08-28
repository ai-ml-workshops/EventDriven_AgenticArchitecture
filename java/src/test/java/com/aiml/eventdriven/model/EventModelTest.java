package com.aiml.eventdriven.model;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class EventModelTest {

    @Test
    public void testTaskRequestEventSerialization() {
        TaskRequestEvent event = TaskRequestEvent.create(
                "Review code changes",
                TaskType.CODE_REVIEW,
                "alice",
                Map.of("pr", 42),
                Map.of("temp", 0.5)
        );

        assertNotNull(event.getEventId());
        assertNotNull(event.getCorrelationId());
        assertEquals("CODE_REVIEW", event.getTaskType());
        assertEquals("alice", event.getUserId());
        assertEquals("Review code changes", event.getPayload().getPrompt());
        assertEquals("1.0.0", event.getSchemaVersion());

        String json = event.toJson();
        assertNotNull(json);

        TaskRequestEvent deserialized = TaskRequestEvent.fromJson(json);
        assertEquals(event.getEventId(), deserialized.getEventId());
        assertEquals(event.getCorrelationId(), deserialized.getCorrelationId());
        assertEquals(event.getPayload().getPrompt(), deserialized.getPayload().getPrompt());
    }

    @Test
    public void testAgentResponseEventSerialization() {
        ActionItem action = new ActionItem("NOTIFY_STAKEHOLDER", "security-room", Map.of("priority", "HIGH"));
        AgentResponseEvent event = AgentResponseEvent.create(
                "corr-123",
                "CODE_REVIEW",
                "Analysis complete: No vulnerabilities found.",
                "claude-3-5-sonnet-20241022",
                "test-agent",
                TaskStatus.SUCCESS,
                120,
                45,
                210,
                true,
                List.of(action)
        );

        assertEquals("corr-123", event.getCorrelationId());
        assertEquals("test-agent", event.getAgentId());
        assertEquals("SUCCESS", event.getStatus());
        assertEquals(165, event.getUsage().getTotalTokens());
        assertEquals(210, event.getMetadata().getLatencyMs());
        assertTrue(event.getMetadata().isActionRequired());
        assertEquals(1, event.getMetadata().getActions().size());

        String json = event.toJson();
        AgentResponseEvent deserialized = AgentResponseEvent.fromJson(json);
        assertEquals(event.getEventId(), deserialized.getEventId());
        assertEquals(event.getCorrelationId(), deserialized.getCorrelationId());
        assertEquals(165, deserialized.getUsage().getTotalTokens());
        assertEquals("security-room", deserialized.getMetadata().getActions().get(0).getTarget());
    }
}
