package com.aiml.eventdriven.subscriber;

import com.aiml.eventdriven.config.PipelineConfig;
import com.aiml.eventdriven.model.ActionItem;
import com.aiml.eventdriven.model.AgentResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ActionNotificationSubscriber implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ActionNotificationSubscriber.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class ExecutedAction {
        private final String actionType;
        private final String target;
        private final String correlationId;
        private final String status;
        private final Map<String, Object> details;

        public ExecutedAction(String actionType, String target, String correlationId, String status, Map<String, Object> details) {
            this.actionType = actionType;
            this.target = target;
            this.correlationId = correlationId;
            this.status = status;
            this.details = details != null ? details : new HashMap<>();
        }

        public String getActionType() { return actionType; }
        public String getTarget() { return target; }
        public String getCorrelationId() { return correlationId; }
        public String getStatus() { return status; }
        public Map<String, Object> getDetails() { return details; }
    }

    private final PipelineConfig config;
    private final String subscriberId;
    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final boolean ownsResources;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<ExecutedAction> executedActions = Collections.synchronizedList(new ArrayList<>());

    public ActionNotificationSubscriber(PipelineConfig config) {
        this(config, createKafkaConsumer(config), createKafkaProducer(config), "action-notification-subscriber-java", true);
    }

    public ActionNotificationSubscriber(PipelineConfig config, Consumer<String, String> consumer, Producer<String, String> producer, String subscriberId) {
        this(config, consumer, producer, subscriberId, false);
    }

    private ActionNotificationSubscriber(PipelineConfig config, Consumer<String, String> consumer, Producer<String, String> producer, String subscriberId, boolean ownsResources) {
        this.config = config;
        this.consumer = consumer;
        this.producer = producer;
        this.subscriberId = subscriberId;
        this.ownsResources = ownsResources;
    }

    private static Consumer<String, String> createKafkaConsumer(PipelineConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getActionConsumerGroup());
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

    private ExecutedAction executeAction(ActionItem action, AgentResponseEvent response) {
        logger.info("[{}] Executing action '{}' for correlationId={}, target='{}'",
                subscriberId, action.getActionType(), response.getCorrelationId(), action.getTarget());

        Map<String, Object> details = new HashMap<>();
        details.put("taskType", response.getTaskType());
        details.put("agentId", response.getAgentId());
        details.put("parameters", action.getParameters());

        ExecutedAction exec = new ExecutedAction(
                action.getActionType(),
                action.getTarget(),
                response.getCorrelationId(),
                "COMPLETED",
                details
        );
        executedActions.add(exec);

        // Forward notification event
        if (producer != null) {
            try {
                Map<String, Object> notif = new HashMap<>();
                notif.put("correlationId", response.getCorrelationId());
                notif.put("actionType", action.getActionType());
                notif.put("target", action.getTarget());
                notif.put("status", "COMPLETED");
                notif.put("summary", "Agent " + response.getAgentId() + " triggered " + action.getActionType());

                producer.send(new ProducerRecord<>(config.getTaskNotificationsTopic(), response.getCorrelationId(), MAPPER.writeValueAsString(notif)));
            } catch (Exception e) {
                logger.debug("Failed to forward notification: {}", e.getMessage());
            }
        }

        return exec;
    }

    public List<ExecutedAction> processResponse(AgentResponseEvent response) {
        List<ExecutedAction> results = new ArrayList<>();
        if (response.getMetadata() != null && response.getMetadata().isActionRequired() && response.getMetadata().getActions() != null) {
            for (ActionItem action : response.getMetadata().getActions()) {
                ExecutedAction exec = executeAction(action, response);
                results.add(exec);
            }
        } else {
            logger.info("[{}] No action required for response {} (correlationId={})",
                    subscriberId, response.getEventId(), response.getCorrelationId());
        }
        return results;
    }

    public List<ExecutedAction> run(int maxMessages) {
        List<ExecutedAction> allActions = new ArrayList<>();
        if (consumer == null) return allActions;

        consumer.subscribe(List.of(config.getAgentResponsesTopic()));
        running.set(true);
        logger.info("[{}] Subscribed to topic '{}'", subscriberId, config.getAgentResponsesTopic());

        try {
            while (running.get() && allActions.size() < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        AgentResponseEvent responseEvent = AgentResponseEvent.fromJson(record.value());
                        List<ExecutedAction> executed = processResponse(responseEvent);
                        allActions.addAll(executed);
                        if (allActions.size() >= maxMessages) {
                            break;
                        }
                    } catch (Exception ex) {
                        logger.error("[{}] Error processing record: {}", subscriberId, ex.getMessage(), ex);
                    }
                }
            }
        } finally {
            stop();
        }

        return allActions;
    }

    public void stop() {
        running.set(false);
    }

    public List<ExecutedAction> getExecutedActions() {
        return executedActions;
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
