# Event-Driven Agentic Pipeline

A production-ready, event-driven pipeline for agentic workflows powered by **Apache Kafka** and **Anthropic Claude LLM**, with implementations in both **Java** and **Python**.

---

## 🌟 Key Features

- **Event Broker**: Local containerized Apache Kafka in KRaft mode (no Zookeeper required) via Docker Compose.
- **Event Producer**: Emits strongly-typed task request events (`TaskRequestEvent`) with correlation IDs and execution contexts.
- **Agent Workflow**: Asynchronous agent consuming task events, interacting with **Anthropic Claude** (or intelligent Mock LLM for offline testability), detecting action items, and producing structured responses (`AgentResponseEvent`).
- **2 Event Subscribers**:
  - **Subscriber 1 (Audit & Analytics Subscriber)**: Tracks execution telemetry, token consumption, and latency metrics; outputs audit trail.
  - **Subscriber 2 (Action & Notification Subscriber)**: Evaluates agent results and executes downstream actions (notifications, alerts, automated triggers).
- **Dual Implementations**: Full feature parity across **Java 17 (Maven)** and **Python 3.9+**.
- **Formal Schemas**: JSON Schema specifications in [`schemas/`](schemas/).

---

## 📐 Architecture & Event Flow

```
[Event Producer]
       │
       │ (publishes TaskRequestEvent)
       ▼
[Kafka Topic: task.requests]
       │
       ▼
[Event-Driven Agent] ◄──► [LLM Service: Claude (Anthropic API / Mock)]
       │
       │ (publishes AgentResponseEvent)
       ▼
[Kafka Topic: agent.responses]
       │
       ├─────────────────────────────────────────┐
       ▼                                         ▼
[Subscriber 1: Audit & Analytics]     [Subscriber 2: Action & Notification]
- Logs audit trail & metrics          - Dispatches alerts/notifications
- Forwards to `audit.logs`            - Forwards to `task.notifications`
```

For detailed architecture diagrams and schema specifications, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 🚀 Quick Start

### 1. Start the Kafka Event Broker (Local Container)

```bash
docker compose up -d
```

This starts Apache Kafka (KRaft mode) on port `9092` and initializes the required topics (`task.requests`, `agent.responses`, `task.notifications`, `audit.logs`).

To check container health:
```bash
docker compose ps
```

---

### 2. Run the Python Pipeline

#### Install Dependencies
```bash
pip install -r python/requirements.txt
```

#### Run Pipeline (with Mock Claude or Real Anthropic API)
```bash
# Using Mock Claude (offline / test mode):
./scripts/run_python_pipeline.sh --mock-llm --tasks 3

# Using Real Anthropic Claude API:
export ANTHROPIC_API_KEY="your-api-key"
./scripts/run_python_pipeline.sh --tasks 3
```

#### Run Python Test Suite
```bash
PYTHONPATH=. pytest -v
```

---

### 3. Run the Java Pipeline

#### Build and Run
```bash
# Using Mock Claude (offline / test mode):
./scripts/run_java_pipeline.sh --mock-llm --tasks 3

# Using Real Anthropic Claude API:
export ANTHROPIC_API_KEY="your-api-key"
./scripts/run_java_pipeline.sh --tasks 3
```

#### Run Java Test Suite (Maven)
```bash
cd java && mvn test
```

---

## ⚙️ Configuration & Environment Variables

| Variable | Description | Default |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker bootstrap servers | `localhost:9092` |
| `TASK_REQUESTS_TOPIC` | Inbound topic for task requests | `task.requests` |
| `AGENT_RESPONSES_TOPIC` | Outbound topic for agent responses | `agent.responses` |
| `TASK_NOTIFICATIONS_TOPIC` | Topic for action & notification events | `task.notifications` |
| `AUDIT_LOGS_TOPIC` | Topic for audit telemetry events | `audit.logs` |
| `ANTHROPIC_API_KEY` | Anthropic API key for Claude | `""` (defaults to mock) |
| `LLM_MODEL` | Claude model identifier | `claude-3-5-sonnet-20241022` |
| `USE_MOCK_LLM` | Force using Mock Claude LLM | `false` |
| `MAX_TOKENS` | Max tokens for LLM generation | `1024` |
| `TEMPERATURE` | Sampling temperature | `0.7` |

---

## 📁 Repository Structure

```
.
├── docker-compose.yml              # Local Kafka container configuration (KRaft mode)
├── ARCHITECTURE.md                 # Detailed architecture & event flow documentation
├── schemas/
│   ├── task-request-schema.json    # JSON Schema for TaskRequestEvent
│   └── agent-response-schema.json  # JSON Schema for AgentResponseEvent
├── python/
│   ├── requirements.txt            # Python dependencies
│   ├── pyproject.toml              # Project metadata & pytest config
│   ├── src/
│   │   ├── config.py               # Pipeline configuration
│   │   ├── models.py               # Event schemas (Pydantic models)
│   │   ├── llm_client.py           # Claude & Mock LLM clients
│   │   ├── producer.py             # Event Producer
│   │   ├── agent.py                # Event-Driven Agent
│   │   ├── subscriber_audit.py     # Subscriber 1: Audit & Analytics
│   │   ├── subscriber_action.py    # Subscriber 2: Action & Notification
│   │   └── pipeline_runner.py      # End-to-end pipeline runner CLI
│   └── tests/                      # Unit & integration tests
├── java/
│   ├── pom.xml                     # Maven project descriptor
│   └── src/
│       ├── main/java/com/aiml/eventdriven/
│       │   ├── config/             # Pipeline configuration
│       │   ├── model/              # Event models & Jackson mapping
│       │   ├── llm/                # Claude & Mock LLM clients
│       │   ├── producer/           # Event Producer
│       │   ├── agent/              # Event-Driven Agent
│       │   ├── subscriber/         # 2 Event Subscribers
│       │   └── PipelineApplication.java
│       └── test/java/com/aiml/eventdriven/ # Unit & integration tests
└── scripts/
    ├── run_python_pipeline.sh      # Python runner script
    └── run_java_pipeline.sh        # Java runner script
```

