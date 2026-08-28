package com.aiml.eventdriven.subscriber;

import com.aiml.eventdriven.config.PipelineConfig;
import com.aiml.eventdriven.model.ActionItem;
import com.aiml.eventdriven.model.AgentResponseEvent;
import com.aiml.eventdriven.model.TaskStatus;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SubscribersTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testAuditLogSubscriber() {
        Producer<String, String> mockProducer = Mockito.mock(Producer.class);
        PipelineConfig config = new PipelineConfig();
        config.setAuditLogsTopic("test.audit.logs");

        AuditLogSubscriber auditSubscriber = new AuditLogSubscriber(config, null, mockProducer, "audit-sub-test");

        AgentResponseEvent response = AgentResponseEvent.create(
                "corr-audit-1",
                "RESEARCH",
                "Research results on event streaming.",
                "claude-3-5-sonnet-20241022",
                "agent-java",
                TaskStatus.SUCCESS,
                80,
                30,
                150,
                false,
                null
        );

        AuditLogSubscriber.AuditRecord record = auditSubscriber.processResponse(response);

        assertEquals("corr-audit-1", record.getCorrelationId());
        assertEquals("agent-java", record.getAgentId());
        assertEquals(110, record.getTotalTokens());
        assertEquals(150, record.getLatencyMs());

        assertEquals(1, auditSubscriber.getMetrics().getTotalProcessed());
        assertEquals(1, auditSubscriber.getMetrics().getSuccessCount());
        assertEquals(110, auditSubscriber.getMetrics().getTotalTokensUsed());
        assertEquals(150.0, auditSubscriber.getMetrics().getAverageLatencyMs());

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture());
        assertEquals("test.audit.logs", captor.getValue().topic());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testActionNotificationSubscriber() {
        Producer<String, String> mockProducer = Mockito.mock(Producer.class);
        PipelineConfig config = new PipelineConfig();
        config.setTaskNotificationsTopic("test.notifications");

        ActionNotificationSubscriber actionSubscriber = new ActionNotificationSubscriber(config, null, mockProducer, "action-sub-test");

        ActionItem action1 = new ActionItem("NOTIFY_STAKEHOLDER", "sec-team", Map.of("urgency", "HIGH"));
        ActionItem action2 = new ActionItem("POST_REVIEW_COMMENT", "PR-99", Map.of("approved", true));

        AgentResponseEvent response = AgentResponseEvent.create(
                "corr-action-1",
                "CODE_REVIEW",
                "Review complete with actions required.",
                "claude-3-5-sonnet-20241022",
                "agent-java",
                TaskStatus.SUCCESS,
                100,
                50,
                200,
                true,
                List.of(action1, action2)
        );

        List<ActionNotificationSubscriber.ExecutedAction> executed = actionSubscriber.processResponse(response);

        assertEquals(2, executed.size());
        assertEquals("NOTIFY_STAKEHOLDER", executed.get(0).getActionType());
        assertEquals("sec-team", executed.get(0).getTarget());
        assertEquals("POST_REVIEW_COMMENT", executed.get(1).getActionType());

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer, times(2)).send(captor.capture());
    }
}
