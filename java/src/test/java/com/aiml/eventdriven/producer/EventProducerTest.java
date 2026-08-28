package com.aiml.eventdriven.producer;

import com.aiml.eventdriven.config.PipelineConfig;
import com.aiml.eventdriven.model.TaskRequestEvent;
import com.aiml.eventdriven.model.TaskType;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

public class EventProducerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testSendEvent() {
        Producer<String, String> mockProducer = Mockito.mock(Producer.class);
        PipelineConfig config = new PipelineConfig();
        config.setTaskRequestsTopic("custom.task.requests");

        EventProducer eventProducer = new EventProducer(config, mockProducer);

        TaskRequestEvent event = TaskRequestEvent.create(
                "Run test suite",
                TaskType.GENERAL_ASSISTANT,
                "tester",
                Map.of("env", "staging"),
                null
        );

        eventProducer.sendEvent(event);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture());

        ProducerRecord<String, String> sentRecord = captor.getValue();
        assertEquals("custom.task.requests", sentRecord.topic());
        assertEquals(event.getCorrelationId(), sentRecord.key());
        assertTrue(sentRecord.value().contains("Run test suite"));
    }
}
