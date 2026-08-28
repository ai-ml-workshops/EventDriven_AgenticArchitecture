package com.aiml.eventdriven.agent;

import com.aiml.eventdriven.config.PipelineConfig;
import com.aiml.eventdriven.llm.MockClaudeLlmClient;
import com.aiml.eventdriven.model.AgentResponseEvent;
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

public class EventDrivenAgentTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testAgentProcessEvent() {
        Producer<String, String> mockProducer = Mockito.mock(Producer.class);
        PipelineConfig config = new PipelineConfig();
        config.setAgentResponsesTopic("test.agent.responses");

        MockClaudeLlmClient mockLlm = new MockClaudeLlmClient("claude-test-model");
        EventDrivenAgent agent = new EventDrivenAgent(config, mockLlm, null, mockProducer, "agent-007");

        TaskRequestEvent request = TaskRequestEvent.create(
                "Review pull request #15 for bugs",
                TaskType.CODE_REVIEW,
                "dev-1",
                Map.of("pr_id", 15),
                null
        );

        AgentResponseEvent response = agent.processEvent(request);

        assertEquals(request.getCorrelationId(), response.getCorrelationId());
        assertEquals("agent-007", response.getAgentId());
        assertEquals("SUCCESS", response.getStatus());
        assertTrue(response.getResponse().contains("CODE_REVIEW"));
        assertTrue(response.getUsage().getTotalTokens() > 0);
        assertTrue(response.getMetadata().isActionRequired());

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture());
        assertEquals("test.agent.responses", captor.getValue().topic());
    }
}
