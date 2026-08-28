# Event-Driven Agentic Pipeline Architecture

This document describes the architectural design, component interactions, event flow, schemas, and LLM integrations for the Event-Driven Agentic Architecture.

---

## 1. Architecture Overview

The system implements an asynchronous event-driven pipeline designed for autonomous AI agents. By decoupling task generation, reasoning, and downstream actions via an event broker (Apache Kafka), the architecture ensures high throughput, fault tolerance, scalability, and distributed traceability.

### Component Diagram

```
+-----------------------------------------------------------------------------+
|                                EVENT BROKER                                 |
|                         (Apache Kafka in KRaft Mode)                        |
|                                                                             |
|  Topics:                                                                    |
|    - task.requests          (Inbound tasks for agent execution)             |
|    - agent.responses         (Outbound agent results & metadata)             |
|    - audit.logs              (Subscriber 1 audit telemetry stream)          |
|    - task.notifications      (Subscriber 2 action & alert stream)           |
+-----------------------------------------------------------------------------+
          ^                                   |                    |
          | Publish                           | Consume            | Consume
          | TaskRequestEvent                  v                    v
+-----------------------+           +-------------------+  +------------------+
|    EVENT PRODUCER     |           |   SUBSCRIBER 1    |  |   SUBSCRIBER 2   |
| (Java & Python SDKs)  |           | (Audit & Metrics) |  | (Action & Alert) |
+-----------------------+           +-------------------+  +------------------+
                                              ^                    ^
                                              |                    |
                                              | Publish            | Publish
                                     +-------------------+         |
                                     |  AGENT WORKFLOW   |---------+
                                     |  (Java / Python)  |
                                     +-------------------+
                                              |
                                              | API Call / Prompt
                                              v
                                     +-------------------+
                                     |    LLM SERVICE    |
                                     | (Claude 3.5 /     |
                                     |  Anthropic API)   |
                                     +-------------------+
```

---

## 2. Pipeline Components

### 2.1 Event Broker (Apache Kafka)
- **Deployment**: Local container via Docker Compose running Kafka in KRaft mode (no Zookeeper required).
- **Partitioning & Ordering**: Keyed partitioning by `correlationId` ensures per-workflow sequential ordering while supporting horizontal scaling.
- **Durable Logging**: Retains task requests and agent reasoning for replayability and auditability.

### 2.2 Event Producer
- Generates `TaskRequestEvent` instances with unique `eventId`, distributed `correlationId`, timestamp, task type, user context, and prompt parameters.
- Publishes to the `task.requests` topic.
- Implemented in both **Java** and **Python**.

### 2.3 Event-Driven Agent
- Consumes `TaskRequestEvent` from `task.requests`.
- Constructs system instructions, formats contextual payloads, and executes reasoning via the **LLM Client (Anthropic Claude)**.
- Analyzes LLM reasoning to identify required side effects (e.g. notifications, PR comments, external triggers).
- Emits structured `AgentResponseEvent` to `agent.responses`.
- Supports configurable model parameters (`model`, `max_tokens`, `temperature`) and fallback to mock mode for offline testing.

### 2.4 Event Subscribers

#### Subscriber 1: Audit and Analytics Subscriber (`AuditLogSubscriber`)
- Consumes `AgentResponseEvent` from `agent.responses` under the `audit-group`.
- Extracts token usage (`promptTokens`, `completionTokens`, `totalTokens`), execution latency (`latencyMs`), and agent status.
- Maintains rolling aggregation metrics (throughput, token consumption, latency averages) and records an immutable audit log.
- Forwards telemetry to `audit.logs`.

#### Subscriber 2: Action and Notification Subscriber (`ActionNotificationSubscriber`)
- Consumes `AgentResponseEvent` from `agent.responses` under the `action-group`.
- Evaluates `metadata.actionRequired` and iterates through planned `actions`.
- Executes downstream actions (e.g. notifying stakeholders via Slack/webhook, dispatching CI/CD comments).
- Emits event confirmation to `task.notifications`.

---

## 3. Event Flow Lifecycle

```
1. Client / Producer
   │
   ├──> Creates TaskRequestEvent
   │    [correlationId: "c1", taskType: "CODE_REVIEW", prompt: "..."]
   │
   └──> Publishes to Kafka Topic: [task.requests]

2. Kafka Broker [task.requests]
   │
   └──> Routes message to Agent Consumer Group

3. Event-Driven Agent
   │
   ├──> Deserializes TaskRequestEvent
   ├──> Calls Anthropic Claude API (messages endpoint)
   │    └──> Receives structured LLM reasoning & token counts
   ├──> Extracts required action items
   ├──> Constructs AgentResponseEvent
   │    [correlationId: "c1", status: "SUCCESS", response: "...", usage: {...}]
   │
   └──> Publishes to Kafka Topic: [agent.responses]

4. Kafka Broker [agent.responses]
   │
   ├──> Delivers message to Subscriber 1 (Audit & Analytics)
   │    └──> Logs telemetry, records token metrics, forwards to [audit.logs]
   │
   └──> Delivers message to Subscriber 2 (Action & Notification)
        └──> Executes actions, dispatches alerts, forwards to [task.notifications]
```

---

## 4. Event Schemas

JSON Schemas are formally defined in the `/schemas` folder:
- [`schemas/task-request-schema.json`](schemas/task-request-schema.json)
- [`schemas/agent-response-schema.json`](schemas/agent-response-schema.json)

### Sample `TaskRequestEvent`
```json
{
  "eventId": "a9c05af5-8ad2-4300-897a-4e35b824ade7",
  "correlationId": "4b065ced-8383-4f47-8410-ce883d92b01d",
  "timestamp": "2026-08-28T04:30:00Z",
  "taskType": "CODE_REVIEW",
  "userId": "developer-alice",
  "payload": {
    "prompt": "Review the authentication middleware for potential security vulnerabilities.",
    "context": {
      "pr_id": "PR-104",
      "notify_channel": "sec-alerts"
    },
    "parameters": {
      "temperature": 0.2,
      "max_tokens": 1024
    }
  },
  "schemaVersion": "1.0.0"
}
```

### Sample `AgentResponseEvent`
```json
{
  "eventId": "597fda23-dfc6-416c-8d34-d383df475203",
  "correlationId": "4b065ced-8383-4f47-8410-ce883d92b01d",
  "timestamp": "2026-08-28T04:30:02Z",
  "agentId": "claude-workflow-agent-python",
  "taskType": "CODE_REVIEW",
  "status": "SUCCESS",
  "response": "Code review completed. Findings: 1. Input validation present. 2. Token refresh safe.",
  "model": "claude-3-5-sonnet-20241022",
  "usage": {
    "promptTokens": 140,
    "completionTokens": 65,
    "totalTokens": 205
  },
  "metadata": {
    "latencyMs": 850,
    "actionRequired": true,
    "actions": [
      {
        "actionType": "POST_REVIEW_COMMENT",
        "target": "PR-104",
        "parameters": {
          "comment": "Automated code review findings ready."
        }
      }
    ]
  },
  "schemaVersion": "1.0.0"
}
```
