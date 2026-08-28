package com.aiml.eventdriven.integration;

import com.aiml.eventdriven.agent.EventDrivenAgent;
import com.aiml.eventdriven.config.PipelineConfig;
import com.aiml.eventdriven.llm.MockClaudeLlmClient;
import com.aiml.eventdriven.model.AgentResponseEvent;
import com.aiml.eventdriven.model.TaskRequestEvent;
import com.aiml.eventdriven.model.TaskType;
import com.aiml.eventdriven.producer.EventProducer;
import com.aiml.eventdriven.subscriber.ActionNotificationSubscriber;
import com.aiml.eventdriven.subscriber.AuditLogSubscriber;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class PipelineIntegrationTest {

    static class InMemoryBroker {
        private final Map<String, List<ProducerRecord<String, String>>> topics = new HashMap<>();

        public synchronized void send(ProducerRecord<String, String> record) {
            topics.computeIfAbsent(record.topic(), k -> new ArrayList<>()).add(record);
        }

        public synchronized List<ProducerRecord<String, String>> getRecords(String topic) {
            return topics.getOrDefault(topic, new ArrayList<>());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFullPipelineEndToEndFlow() {
        InMemoryBroker broker = new InMemoryBroker();
        Producer<String, String> mockProducer = Mockito.mock(Producer.class);

        doAnswer(invocation -> {
            ProducerRecord<String, String> rec = invocation.getArgument(0);
            broker.send(rec);
            RecordMetadata metadata = new RecordMetadata(new TopicPartition(rec.topic(), 0), 0, 0, System.currentTimeMillis(), 0, 0);
            return CompletableFuture.completedFuture(metadata);
        }).when(mockProducer).send(any(ProducerRecord.class));

        PipelineConfig config = new PipelineConfig();
        config.setTaskRequestsTopic("task.requests");
        config.setAgentResponsesTopic("agent.responses");
        config.setAuditLogsTopic("audit.logs");
        config.setTaskNotificationsTopic("task.notifications");

        // 1. Initialize Producer
        EventProducer producer = new EventProducer(config, mockProducer);

        // 2. Initialize Agent
        MockClaudeLlmClient mockLlm = new MockClaudeLlmClient("claude-3-5-sonnet-20241022-mock");
        EventDrivenAgent agent = new EventDrivenAgent(config, mockLlm, null, mockProducer, "agent-integration-java");

        // 3. Initialize 2 Subscribers
        AuditLogSubscriber auditSubscriber = new AuditLogSubscriber(config, null, mockProducer, "audit-sub-java");
        ActionNotificationSubscriber actionSubscriber = new ActionNotificationSubscriber(config, null, mockProducer, "action-sub-java");

        // 4. Produce 3 Tasks
        producer.createAndSendTask("Review authentication logic in PR #10", TaskType.CODE_REVIEW, "java-user", Map.of("pr_id", 10), null);
        producer.createAndSendTask("Research stream processing topologies", TaskType.RESEARCH, "java-user", null, null);
        producer.createAndSendTask("Create migration action plan", TaskType.ACTION_PLANNING, "java-user", Map.of("notify_channel", "devs"), null);

        List<ProducerRecord<String, String>> reqRecords = broker.getRecords("task.requests");
        assertEquals(3, reqRecords.size());

        // 5. Agent processes each request
        List<AgentResponseEvent> agentResponses = new ArrayList<>();
        for (ProducerRecord<String, String> record : reqRecords) {
            TaskRequestEvent req = TaskRequestEvent.fromJson(record.value());
            AgentResponseEvent res = agent.processEvent(req);
            agentResponses.add(res);
        }

        List<ProducerRecord<String, String>> respRecords = broker.getRecords("agent.responses");
        assertEquals(3, respRecords.size());

        // 6. Both Subscribers consume each agent response
        for (ProducerRecord<String, String> record : respRecords) {
            AgentResponseEvent res = AgentResponseEvent.fromJson(record.value());
            auditSubscriber.processResponse(res);
            actionSubscriber.processResponse(res);
        }

        // 7. Verification on Subscriber 1 (Audit Log Subscriber)
        assertEquals(3, auditSubscriber.getMetrics().getTotalProcessed());
        assertEquals(3, auditSubscriber.getMetrics().getSuccessCount());
        assertTrue(auditSubscriber.getMetrics().getTotalTokensUsed() > 0);
        assertTrue(auditSubscriber.getMetrics().getAverageLatencyMs() >= 0);

        // 8. Verification on Subscriber 2 (Action Subscriber)
        assertTrue(actionSubscriber.getExecutedActions().size() >= 2);
    }
}
