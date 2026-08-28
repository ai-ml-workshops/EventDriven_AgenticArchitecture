"""End-to-End Pipeline Runner for Python Agentic Architecture."""

import argparse
import logging
import sys
import threading
import time

from .agent import EventDrivenAgent
from .config import PipelineConfig
from .llm_client import get_llm_client
from .models import TaskType
from .producer import EventProducer
from .subscriber_action import ActionNotificationSubscriber
from .subscriber_audit import AuditLogSubscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("PipelineRunner")


def run_pipeline(config: PipelineConfig, num_tasks: int = 3):
    """Runs producer, agent, and both subscribers concurrently against Kafka."""
    logger.info("Initializing Agentic Event Pipeline components...")

    llm_client = get_llm_client(config)
    producer = EventProducer(config=config)
    agent = EventDrivenAgent(config=config, llm_client=llm_client)
    audit_sub = AuditLogSubscriber(config=config)
    action_sub = ActionNotificationSubscriber(config=config)

    # Start subscribers and agent in background threads
    threads = [
        threading.Thread(target=audit_sub.run, kwargs={"max_messages": num_tasks}, daemon=True, name="AuditSubscriber"),
        threading.Thread(target=action_sub.run, kwargs={"max_messages": num_tasks}, daemon=True, name="ActionSubscriber"),
        threading.Thread(target=agent.run, kwargs={"max_messages": num_tasks}, daemon=True, name="Agent"),
    ]

    for t in threads:
        t.start()

    time.sleep(2)  # Allow consumers to join groups

    logger.info(f"Producing {num_tasks} sample tasks...")
    sample_tasks = [
        (
            "Review the authentication middleware for potential security vulnerabilities.",
            TaskType.CODE_REVIEW,
            {"pr_id": "PR-104", "notify_channel": "sec-alerts"},
        ),
        (
            "Analyze trade-offs between Kafka log compaction and retention policies.",
            TaskType.RESEARCH,
            {"notify_channel": "arch-team"},
        ),
        (
            "Create an action plan to migrate legacy synchronous batch processing to event-driven streams.",
            TaskType.ACTION_PLANNING,
            {"notify_channel": "devops-leads"},
        ),
    ]

    for i in range(num_tasks):
        prompt, task_type, context = sample_tasks[i % len(sample_tasks)]
        producer.create_and_send_task(
            prompt=prompt,
            task_type=task_type,
            user_id=f"user-{i+1}",
            context=context,
        )
        time.sleep(1)

    producer.flush()

    # Wait for completion
    logger.info("Waiting for pipeline events to be fully processed...")
    for t in threads:
        t.join(timeout=15)

    logger.info("Pipeline execution finished.")
    logger.info(f"Audit Metrics: {audit_sub.metrics}")
    logger.info(f"Executed Actions: {len(action_sub.executed_actions)}")


def main():
    parser = argparse.ArgumentParser(description="Run Python Event-Driven Agentic Pipeline")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--tasks", type=int, default=3, help="Number of tasks to produce and process")
    parser.add_argument("--mock-llm", action="store_true", help="Use mock LLM instead of real API")
    args = parser.parse_args()

    config = PipelineConfig(
        kafka_bootstrap_servers=args.broker,
        use_mock_llm=args.mock_llm,
    )

    run_pipeline(config, num_tasks=args.tasks)


if __name__ == "__main__":
    main()
