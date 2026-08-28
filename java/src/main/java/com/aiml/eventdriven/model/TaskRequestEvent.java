package com.aiml.eventdriven.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRequestEvent {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String eventId;
    private String correlationId;
    private String timestamp;
    private String taskType;
    private String userId;
    private TaskPayload payload;
    private String schemaVersion;

    public TaskRequestEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.correlationId = UUID.randomUUID().toString();
        this.timestamp = Instant.now().toString();
        this.taskType = TaskType.GENERAL_ASSISTANT.name();
        this.userId = "system-user";
        this.schemaVersion = "1.0.0";
    }

    public static TaskRequestEvent create(String prompt, TaskType taskType, String userId, Map<String, Object> context, Map<String, Object> parameters) {
        TaskRequestEvent event = new TaskRequestEvent();
        event.setTaskType(taskType != null ? taskType.name() : TaskType.GENERAL_ASSISTANT.name());
        event.setUserId(userId != null ? userId : "system-user");
        event.setPayload(new TaskPayload(prompt, context, parameters));
        return event;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize TaskRequestEvent to JSON", e);
        }
    }

    public static TaskRequestEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, TaskRequestEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize TaskRequestEvent from JSON", e);
        }
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public TaskPayload getPayload() { return payload; }
    public void setPayload(TaskPayload payload) { this.payload = payload; }

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }
}
