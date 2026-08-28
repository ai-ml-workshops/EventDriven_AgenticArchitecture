package com.aiml.eventdriven.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskPayload {
    private String prompt;
    private Map<String, Object> context = new HashMap<>();
    private Map<String, Object> parameters = new HashMap<>();

    public TaskPayload() {}

    public TaskPayload(String prompt) {
        this.prompt = prompt;
    }

    public TaskPayload(String prompt, Map<String, Object> context, Map<String, Object> parameters) {
        this.prompt = prompt;
        this.context = context != null ? context : new HashMap<>();
        this.parameters = parameters != null ? parameters : new HashMap<>();
    }

    public String getPrompt() { return prompt; }
    public void setPrompt(String prompt) { this.prompt = prompt; }

    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> context) { this.context = context != null ? context : new HashMap<>(); }

    public Map<String, Object> getParameters() { return parameters; }
    public void setParameters(Map<String, Object> parameters) { this.parameters = parameters != null ? parameters : new HashMap<>(); }
}
