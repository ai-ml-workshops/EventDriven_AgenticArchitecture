package com.aiml.eventdriven;

import com.aiml.eventdriven.agent.EventDrivenAgent;
import com.aiml.eventdriven.config.PipelineConfig;
import com.aiml.eventdriven.llm.LlmClient;
import com.aiml.eventdriven.llm.LlmClientFactory;
import com.aiml.eventdriven.model.TaskType;
import com.aiml.eventdriven.producer.EventProducer;
import com.aiml.eventdriven.subscriber.ActionNotificationSubscriber;
import com.aiml.eventdriven.subscriber.AuditLogSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PipelineApplication {
    private static final Logger logger = LoggerFactory.getLogger(PipelineApplication.class);

    public static void main(String[] args) {
        logger.info("Starting Java Event-Driven Agentic Pipeline...");

        PipelineConfig config = new PipelineConfig();
        int taskCount = 3;

        for (int i = 0; i < args.length; i++) {
            if ("--broker".equals(args[i]) && i + 1 < args.length) {
                config.setKafkaBootstrapServers(args[++i]);
            } else if ("--tasks".equals(args[i]) && i + 1 < args.length) {
                taskCount = Integer.parseInt(args[++i]);
            } else if ("--mock-llm".equals(args[i])) {
                config.setUseMockLlm(true);
            }
        }

        LlmClient llmClient = LlmClientFactory.create(config);
        EventProducer producer = new EventProducer(config);
        EventDrivenAgent agent = new EventDrivenAgent(config, llmClient, "claude-workflow-agent-java");
        AuditLogSubscriber auditSubscriber = new AuditLogSubscriber(config);
        ActionNotificationSubscriber actionSubscriber = new ActionNotificationSubscriber(config);

        ExecutorService executor = Executors.newFixedThreadPool(3);

        final int finalTaskCount = taskCount;
        CompletableFuture<Void> agentFuture = CompletableFuture.runAsync(() -> agent.run(finalTaskCount), executor);
        CompletableFuture<Void> auditFuture = CompletableFuture.runAsync(() -> auditSubscriber.run(finalTaskCount), executor);
        CompletableFuture<Void> actionFuture = CompletableFuture.runAsync(() -> actionSubscriber.run(finalTaskCount), executor);

        try {
            Thread.sleep(2000); // Allow Kafka consumer groups to rebalance

            logger.info("Producing {} sample tasks to Kafka...", finalTaskCount);

            producer.createAndSendTask(
                    "Review authentication middleware for potential token validation bypasses.",
                    TaskType.CODE_REVIEW,
                    "java-user-1",
                    Map.of("pr_id", "PR-502", "notify_channel", "security-room"),
                    Map.of("temperature", 0.3)
            );
            Thread.sleep(1000);

            producer.createAndSendTask(
                    "Analyze architectural patterns for stateful agent event streams.",
                    TaskType.RESEARCH,
                    "java-user-2",
                    Map.of("notify_channel", "arch-guild"),
                    Map.of("temperature", 0.5)
            );
            Thread.sleep(1000);

            producer.createAndSendTask(
                    "Create an action plan to migrate synchronous services to Kafka events.",
                    TaskType.ACTION_PLANNING,
                    "java-user-3",
                    Map.of("notify_channel", "lead-engineers"),
                    Map.of("temperature", 0.4)
            );

            producer.flush();

            CompletableFuture.allOf(agentFuture, auditFuture, actionFuture).join();
            logger.info("Pipeline processing completed successfully.");
            logger.info("Audit Total Processed: {}", auditSubscriber.getMetrics().getTotalProcessed());
            logger.info("Actions Executed: {}", actionSubscriber.getExecutedActions().size());

        } catch (Exception e) {
            logger.error("Pipeline encountered an error: {}", e.getMessage(), e);
        } finally {
            producer.close();
            agent.close();
            auditSubscriber.close();
            actionSubscriber.close();
            executor.shutdown();
        }
    }
}
