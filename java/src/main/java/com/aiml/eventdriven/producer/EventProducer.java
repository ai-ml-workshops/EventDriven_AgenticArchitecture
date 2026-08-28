package com.aiml.eventdriven.producer;

import com.aiml.eventdriven.config.PipelineConfig;
import com.aiml.eventdriven.model.TaskRequestEvent;
import com.aiml.eventdriven.model.TaskType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class EventProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(EventProducer.class);

    private final PipelineConfig config;
    private final Producer<String, String> producer;
    private final boolean ownsProducer;

    public EventProducer(PipelineConfig config) {
        this(config, createKafkaProducer(config), true);
    }

    public EventProducer(PipelineConfig config, Producer<String, String> producer) {
        this(config, producer, false);
    }

    private EventProducer(PipelineConfig config, Producer<String, String> producer, boolean ownsProducer) {
        this.config = config;
        this.producer = producer;
        this.ownsProducer = ownsProducer;
    }

    private static Producer<String, String> createKafkaProducer(PipelineConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new KafkaProducer<>(props);
    }

    public Future<RecordMetadata> sendEvent(TaskRequestEvent event) {
        String topic = config.getTaskRequestsTopic();
        String key = event.getCorrelationId();
        String jsonValue = event.toJson();

        logger.info("Producing event {} (correlationId={}, taskType={}) to topic '{}'",
                event.getEventId(), key, event.getTaskType(), topic);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
        return producer.send(record);
    }

    public TaskRequestEvent createAndSendTask(
            String prompt,
            TaskType taskType,
            String userId,
            Map<String, Object> context,
            Map<String, Object> parameters) {
        TaskRequestEvent event = TaskRequestEvent.create(prompt, taskType, userId, context, parameters);
        sendEvent(event);
        return event;
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        if (ownsProducer && producer != null) {
            producer.close();
        }
    }
}
