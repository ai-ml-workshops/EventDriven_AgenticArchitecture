package com.aiml.eventdriven.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResponseMetadata {
    private long latencyMs;
    private boolean actionRequired;
    private List<ActionItem> actions = new ArrayList<>();

    public ResponseMetadata() {}

    public ResponseMetadata(long latencyMs, boolean actionRequired, List<ActionItem> actions) {
        this.latencyMs = latencyMs;
        this.actionRequired = actionRequired;
        this.actions = actions != null ? actions : new ArrayList<>();
    }

    public long getLatencyMs() { return latencyMs; }
    public void setLatencyMs(long latencyMs) { this.latencyMs = latencyMs; }

    public boolean isActionRequired() { return actionRequired; }
    public void setActionRequired(boolean actionRequired) { this.actionRequired = actionRequired; }

    public List<ActionItem> getActions() { return actions; }
    public void setActions(List<ActionItem> actions) { this.actions = actions != null ? actions : new ArrayList<>(); }
}
