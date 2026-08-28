package com.aiml.eventdriven.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ActionItem {
    private String actionType;
    private String target;
    private Map<String, Object> parameters = new HashMap<>();

    public ActionItem() {}

    public ActionItem(String actionType, String target, Map<String, Object> parameters) {
        this.actionType = actionType;
        this.target = target;
        this.parameters = parameters != null ? parameters : new HashMap<>();
    }

    public String getActionType() { return actionType; }
    public void setActionType(String actionType) { this.actionType = actionType; }

    public String getTarget() { return target; }
    public void setTarget(String target) { this.target = target; }

    public Map<String, Object> getParameters() { return parameters; }
    public void setParameters(Map<String, Object> parameters) { this.parameters = parameters != null ? parameters : new HashMap<>(); }
}
