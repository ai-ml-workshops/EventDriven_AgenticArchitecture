package com.aiml.eventdriven.agent;

import com.aiml.eventdriven.config.PipelineConfig;
import com.aiml.eventdriven.llm.LlmClient;
import com.aiml.eventdriven.llm.LlmClientFactory;
import com.aiml.eventdriven.llm.LlmResponse;
import com.aiml.eventdriven.model.ActionItem;
import com.aiml.eventdriven.model.AgentResponseEvent;
import com.aiml.eventdriven.model.TaskRequestEvent;
import com.aiml.eventdriven.model.TaskStatus;
import com.aiml.eventdriven.model.TaskType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventDrivenAgent implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(EventDrivenAgent.class);

    private final PipelineConfig config;
    private final String agentId;
    private final LlmClient llmClient;
    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final boolean ownsResources;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public EventDrivenAgent(PipelineConfig config) {
        this(config, LlmClientFactory.create(config), "claude-workflow-agent-java");
    }

    public EventDrivenAgent(PipelineConfig config, LlmClient llmClient, String agentId) {
        this(config, llmClient, createKafkaConsumer(config), createKafkaProducer(config), agentId, true);
    }

    public EventDrivenAgent(
            PipelineConfig config,
            LlmClient llmClient,
            Consumer<String, String> consumer,
            Producer<String, String> producer,
            String agentId) {
        this(config, llmClient, consumer, producer, agentId, false);
    }

    private EventDrivenAgent(
            PipelineConfig config,
            LlmClient llmClient,
            Consumer<String, String> consumer,
            Producer<String, String> producer,
            String agentId,
            boolean ownsResources) {
        this.config = config;
        this.llmClient = llmClient != null ? llmClient : LlmClientFactory.create(config);
        this.agentId = agentId != null ? agentId : "claude-workflow-agent-java";
        this.consumer = consumer;
        this.producer = producer;
        this.ownsResources = ownsResources;
    }

    private static Consumer<String, String> createKafkaConsumer(PipelineConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getAgentConsumerGroup());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }

    private static Producer<String, String> createKafkaProducer(PipelineConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    public List<ActionItem> extractActions(String taskType, String llmOutput, Map<String, Object> context) {
        List<ActionItem> actions = new ArrayList<>();
        String outputLower = llmOutput != null ? llmOutput.toLowerCase() : "";

        if (TaskType.ACTION_PLANNING.name().equalsIgnoreCase(taskType) || outputLower.contains("action") || outputLower.contains("notify")) {
            String target = (context != null && context.containsKey("notify_channel"))
                    ? (String) context.get("notify_channel")
                    : "general-alerts";
            actions.add(new ActionItem("NOTIFY_STAKEHOLDER", target, Map.of("summary", "Agent completed action planning workflow", "priority", "HIGH")));
        }
        if (TaskType.CODE_REVIEW.name().equalsIgnoreCase(taskType) && outputLower.contains("recommendation")) {
            String target = (context != null && context.containsKey("pr_id"))
                    ? String.valueOf(context.get("pr_id"))
                    : "default-pr";
            actions.add(new ActionItem("POST_REVIEW_COMMENT", target, Map.of("comment", "Automated code review findings ready.")));
        }
        return actions;
    }

    public AgentResponseEvent processEvent(TaskRequestEvent event) {
        logger.info("Agent [{}] processing event {} (taskType={})", agentId, event.getEventId(), event.getTaskType());
        long startTime = System.currentTimeMillis();

        String prompt = event.getPayload() != null ? event.getPayload().getPrompt() : "";
        Map<String, Object> context = event.getPayload() != null ? event.getPayload().getContext() : Collections.emptyMap();
        Map<String, Object> params = event.getPayload() != null ? event.getPayload().getParameters() : Collections.emptyMap();

        String systemPrompt = String.format(
                "You are an expert AI agent (%s) specialized in executing %s workflows. " +
                "Analyze the prompt and provided context carefully, reason through the problem, and provide a clear, structured solution.",
                agentId, event.getTaskType()
        );

        AgentResponseEvent responseEvent;
        try {
            LlmResponse llmResponse = llmClient.generate(prompt, systemPrompt, context, params);
            long latencyMs = System.currentTimeMillis() - startTime;

            List<ActionItem> actions = extractActions(event.getTaskType(), llmResponse.getContent(), context);
            boolean actionRequired = !actions.isEmpty();

            responseEvent = AgentResponseEvent.create(
                    event.getCorrelationId(),
                    event.getTaskType(),
                    llmResponse.getContent(),
                    llmResponse.getModel(),
                    agentId,
                    TaskStatus.SUCCESS,
                    llmResponse.getPromptTokens(),
                    llmResponse.getCompletionTokens(),
                    latencyMs,
                    actionRequired,
                    actions
            );
        } catch (Exception e) {
            long latencyMs = System.currentTimeMillis() - startTime;
            logger.error("Agent [{}] encountered error processing event {}: {}", agentId, event.getEventId(), e.getMessage(), e);
            responseEvent = AgentResponseEvent.create(
                    event.getCorrelationId(),
                    event.getTaskType(),
                    "Error executing task: " + e.getMessage(),
                    llmClient.getModelName(),
                    agentId,
                    TaskStatus.FAILED,
                    0,
                    0,
                    latencyMs,
                    false,
                    Collections.emptyList()
            );
        }

        publishResponse(responseEvent);
        return responseEvent;
    }

    public void publishResponse(AgentResponseEvent responseEvent) {
        if (producer != null) {
            String topic = config.getAgentResponsesTopic();
            String key = responseEvent.getCorrelationId();
            String jsonValue = responseEvent.toJson();

            logger.info("Agent [{}] publishing response {} (correlationId={}, status={}) to topic '{}'",
                    agentId, responseEvent.getEventId(), key, responseEvent.getStatus(), topic);

            producer.send(new ProducerRecord<>(topic, key, jsonValue));
            producer.flush();
        }
    }

    public List<AgentResponseEvent> run(int maxMessages) {
        List<AgentResponseEvent> processed = new ArrayList<>();
        if (consumer == null) return processed;

        consumer.subscribe(List.of(config.getTaskRequestsTopic()));
        running.set(true);
        logger.info("Agent [{}] subscribed to topic '{}'", agentId, config.getTaskRequestsTopic());

        try {
            while (running.get() && processed.size() < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        TaskRequestEvent taskEvent = TaskRequestEvent.fromJson(record.value());
                        AgentResponseEvent response = processEvent(taskEvent);
                        processed.add(response);
                        if (processed.size() >= maxMessages) {
                            break;
                        }
                    } catch (Exception ex) {
                        logger.error("Failed to process Kafka record: {}", ex.getMessage(), ex);
                    }
                }
            }
        } finally {
            stop();
        }

        return processed;
    }

    public void stop() {
        running.set(false);
    }

    public String getAgentId() {
        return agentId;
    }

    public LlmClient getLlmClient() {
        return llmClient;
    }

    @Override
    public void close() {
        stop();
        if (ownsResources) {
            if (consumer != null) consumer.close();
            if (producer != null) producer.close();
        }
    }
}
