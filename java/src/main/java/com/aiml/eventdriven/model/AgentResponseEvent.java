package com.aiml.eventdriven.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AgentResponseEvent {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String eventId;
    private String correlationId;
    private String timestamp;
    private String agentId;
    private String taskType;
    private String status;
    private String response;
    private String model;
    private TokenUsage usage = new TokenUsage();
    private ResponseMetadata metadata = new ResponseMetadata();
    private String schemaVersion;

    public AgentResponseEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now().toString();
        this.agentId = "claude-agent-java";
        this.status = TaskStatus.SUCCESS.name();
        this.schemaVersion = "1.0.0";
    }

    public static AgentResponseEvent create(
            String correlationId,
            String taskType,
            String response,
            String model,
            String agentId,
            TaskStatus status,
            int promptTokens,
            int completionTokens,
            long latencyMs,
            boolean actionRequired,
            List<ActionItem> actions) {
        AgentResponseEvent event = new AgentResponseEvent();
        event.setCorrelationId(correlationId);
        event.setTaskType(taskType);
        event.setResponse(response);
        event.setModel(model);
        if (agentId != null) {
            event.setAgentId(agentId);
        }
        if (status != null) {
            event.setStatus(status.name());
        }
        event.setUsage(new TokenUsage(promptTokens, completionTokens));
        event.setMetadata(new ResponseMetadata(latencyMs, actionRequired, actions));
        return event;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize AgentResponseEvent to JSON", e);
        }
    }

    public static AgentResponseEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, AgentResponseEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize AgentResponseEvent from JSON", e);
        }
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getResponse() { return response; }
    public void setResponse(String response) { this.response = response; }

    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }

    public TokenUsage getUsage() { return usage; }
    public void setUsage(TokenUsage usage) { this.usage = usage; }

    public ResponseMetadata getMetadata() { return metadata; }
    public void setMetadata(ResponseMetadata metadata) { this.metadata = metadata; }

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }
}
