package com.aiml.eventdriven.config;

public class PipelineConfig {

    private String kafkaBootstrapServers;
    private String taskRequestsTopic;
    private String agentResponsesTopic;
    private String taskNotificationsTopic;
    private String auditLogsTopic;
    private String agentConsumerGroup;
    private String auditConsumerGroup;
    private String actionConsumerGroup;
    private String anthropicApiKey;
    private String llmModel;
    private boolean useMockLlm;
    private int maxTokens;
    private double temperature;

    public PipelineConfig() {
        this.kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        this.taskRequestsTopic = getEnvOrDefault("TASK_REQUESTS_TOPIC", "task.requests");
        this.agentResponsesTopic = getEnvOrDefault("AGENT_RESPONSES_TOPIC", "agent.responses");
        this.taskNotificationsTopic = getEnvOrDefault("TASK_NOTIFICATIONS_TOPIC", "task.notifications");
        this.auditLogsTopic = getEnvOrDefault("AUDIT_LOGS_TOPIC", "audit.logs");
        this.agentConsumerGroup = getEnvOrDefault("AGENT_CONSUMER_GROUP", "agentic-pipeline-agent-group-java");
        this.auditConsumerGroup = getEnvOrDefault("AUDIT_CONSUMER_GROUP", "agentic-pipeline-audit-group-java");
        this.actionConsumerGroup = getEnvOrDefault("ACTION_CONSUMER_GROUP", "agentic-pipeline-action-group-java");
        this.anthropicApiKey = getEnvOrDefault("ANTHROPIC_API_KEY", "");
        this.llmModel = getEnvOrDefault("LLM_MODEL", "claude-3-5-sonnet-20241022");
        this.useMockLlm = Boolean.parseBoolean(getEnvOrDefault("USE_MOCK_LLM", "false"));
        this.maxTokens = Integer.parseInt(getEnvOrDefault("MAX_TOKENS", "1024"));
        this.temperature = Double.parseDouble(getEnvOrDefault("TEMPERATURE", "0.7"));
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.trim().isEmpty()) ? val : defaultValue;
    }

    // Getters and Setters
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public void setKafkaBootstrapServers(String kafkaBootstrapServers) { this.kafkaBootstrapServers = kafkaBootstrapServers; }

    public String getTaskRequestsTopic() { return taskRequestsTopic; }
    public void setTaskRequestsTopic(String taskRequestsTopic) { this.taskRequestsTopic = taskRequestsTopic; }

    public String getAgentResponsesTopic() { return agentResponsesTopic; }
    public void setAgentResponsesTopic(String agentResponsesTopic) { this.agentResponsesTopic = agentResponsesTopic; }

    public String getTaskNotificationsTopic() { return taskNotificationsTopic; }
    public void setTaskNotificationsTopic(String taskNotificationsTopic) { this.taskNotificationsTopic = taskNotificationsTopic; }

    public String getAuditLogsTopic() { return auditLogsTopic; }
    public void setAuditLogsTopic(String auditLogsTopic) { this.auditLogsTopic = auditLogsTopic; }

    public String getAgentConsumerGroup() { return agentConsumerGroup; }
    public void setAgentConsumerGroup(String agentConsumerGroup) { this.agentConsumerGroup = agentConsumerGroup; }

    public String getAuditConsumerGroup() { return auditConsumerGroup; }
    public void setAuditConsumerGroup(String auditConsumerGroup) { this.auditConsumerGroup = auditConsumerGroup; }

    public String getActionConsumerGroup() { return actionConsumerGroup; }
    public void setActionConsumerGroup(String actionConsumerGroup) { this.actionConsumerGroup = actionConsumerGroup; }

    public String getAnthropicApiKey() { return anthropicApiKey; }
    public void setAnthropicApiKey(String anthropicApiKey) { this.anthropicApiKey = anthropicApiKey; }

    public String getLlmModel() { return llmModel; }
    public void setLlmModel(String llmModel) { this.llmModel = llmModel; }

    public boolean isUseMockLlm() { return useMockLlm; }
    public void setUseMockLlm(boolean useMockLlm) { this.useMockLlm = useMockLlm; }

    public int getMaxTokens() { return maxTokens; }
    public void setMaxTokens(int maxTokens) { this.maxTokens = maxTokens; }

    public double getTemperature() { return temperature; }
    public void setTemperature(double temperature) { this.temperature = temperature; }
}
