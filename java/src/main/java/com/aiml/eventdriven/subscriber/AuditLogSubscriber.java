package com.aiml.eventdriven.subscriber;

import com.aiml.eventdriven.config.PipelineConfig;
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

public class AuditLogSubscriber implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AuditLogSubscriber.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class AuditRecord {
        private final String eventId;
        private final String correlationId;
        private final String agentId;
        private final String taskType;
        private final String status;
        private final String model;
        private final int totalTokens;
        private final long latencyMs;
        private final String timestamp;

        public AuditRecord(String eventId, String correlationId, String agentId, String taskType, String status, String model, int totalTokens, long latencyMs, String timestamp) {
            this.eventId = eventId;
            this.correlationId = correlationId;
            this.agentId = agentId;
            this.taskType = taskType;
            this.status = status;
            this.model = model;
            this.totalTokens = totalTokens;
            this.latencyMs = latencyMs;
            this.timestamp = timestamp;
        }

        public String getEventId() { return eventId; }
        public String getCorrelationId() { return correlationId; }
        public String getAgentId() { return agentId; }
        public String getTaskType() { return taskType; }
        public String getStatus() { return status; }
        public String getModel() { return model; }
        public int getTotalTokens() { return totalTokens; }
        public long getLatencyMs() { return latencyMs; }
        public String getTimestamp() { return timestamp; }
    }

    public static class AuditMetrics {
        private int totalProcessed = 0;
        private int successCount = 0;
        private int failureCount = 0;
        private int totalTokensUsed = 0;
        private long totalLatencyMs = 0;

        public synchronized void record(boolean success, int tokens, long latencyMs) {
            totalProcessed++;
            if (success) successCount++; else failureCount++;
            totalTokensUsed += tokens;
            totalLatencyMs += latencyMs;
        }

        public synchronized int getTotalProcessed() { return totalProcessed; }
        public synchronized int getSuccessCount() { return successCount; }
        public synchronized int getFailureCount() { return failureCount; }
        public synchronized int getTotalTokensUsed() { return totalTokensUsed; }
        public synchronized double getAverageLatencyMs() {
            return totalProcessed > 0 ? (double) totalLatencyMs / totalProcessed : 0.0;
        }
    }

    private final PipelineConfig config;
    private final String subscriberId;
    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final boolean ownsResources;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<AuditRecord> auditRecords = Collections.synchronizedList(new ArrayList<>());
    private final AuditMetrics metrics = new AuditMetrics();

    public AuditLogSubscriber(PipelineConfig config) {
        this(config, createKafkaConsumer(config), createKafkaProducer(config), "audit-analytics-subscriber-java", true);
    }

    public AuditLogSubscriber(PipelineConfig config, Consumer<String, String> consumer, Producer<String, String> producer, String subscriberId) {
        this(config, consumer, producer, subscriberId, false);
    }

    private AuditLogSubscriber(PipelineConfig config, Consumer<String, String> consumer, Producer<String, String> producer, String subscriberId, boolean ownsResources) {
        this.config = config;
        this.consumer = consumer;
        this.producer = producer;
        this.subscriberId = subscriberId;
        this.ownsResources = ownsResources;
    }

    private static Consumer<String, String> createKafkaConsumer(PipelineConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getAuditConsumerGroup());
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

    public AuditRecord processResponse(AgentResponseEvent response) {
        int tokens = response.getUsage() != null ? response.getUsage().getTotalTokens() : 0;
        long latencyMs = response.getMetadata() != null ? response.getMetadata().getLatencyMs() : 0;
        boolean success = "SUCCESS".equalsIgnoreCase(response.getStatus());

        AuditRecord record = new AuditRecord(
                response.getEventId(),
                response.getCorrelationId(),
                response.getAgentId(),
                response.getTaskType(),
                response.getStatus(),
                response.getModel(),
                tokens,
                latencyMs,
                response.getTimestamp()
        );

        auditRecords.add(record);
        metrics.record(success, tokens, latencyMs);

        logger.info("[{}] AUDIT LOG: eventId={}, correlationId={}, agent={}, status={}, tokens={}, latency={}ms",
                subscriberId, record.getEventId(), record.getCorrelationId(), record.getAgentId(),
                record.getStatus(), record.getTotalTokens(), record.getLatencyMs());

        // Forward to audit.logs topic if producer is available
        if (producer != null) {
            try {
                Map<String, Object> map = new HashMap<>();
                map.put("eventId", record.getEventId());
                map.put("correlationId", record.getCorrelationId());
                map.put("agentId", record.getAgentId());
                map.put("taskType", record.getTaskType());
                map.put("status", record.getStatus());
                map.put("totalTokens", record.getTotalTokens());
                map.put("latencyMs", record.getLatencyMs());
                map.put("timestamp", record.getTimestamp());

                producer.send(new ProducerRecord<>(config.getAuditLogsTopic(), record.getCorrelationId(), MAPPER.writeValueAsString(map)));
            } catch (Exception e) {
                logger.debug("Failed to forward to audit topic: {}", e.getMessage());
            }
        }

        return record;
    }

    public List<AuditRecord> run(int maxMessages) {
        List<AuditRecord> processed = new ArrayList<>();
        if (consumer == null) return processed;

        consumer.subscribe(List.of(config.getAgentResponsesTopic()));
        running.set(true);
        logger.info("[{}] Subscribed to topic '{}'", subscriberId, config.getAgentResponsesTopic());

        try {
            while (running.get() && processed.size() < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        AgentResponseEvent responseEvent = AgentResponseEvent.fromJson(record.value());
                        AuditRecord auditRecord = processResponse(responseEvent);
                        processed.add(auditRecord);
                        if (processed.size() >= maxMessages) {
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

        return processed;
    }

    public void stop() {
        running.set(false);
    }

    public List<AuditRecord> getAuditRecords() {
        return auditRecords;
    }

    public AuditMetrics getMetrics() {
        return metrics;
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
