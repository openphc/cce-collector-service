# CCE Collector Service

> **Release 1.0.0**

The **Collector Service** is the event ingestion gateway of the Care Coordination Engine (CCE). It receives clinical events from external EHR/RHIE systems, validates them as CloudEvents v1.0 envelopes with FHIR R4 or plain JSON payloads, and publishes them to Kafka for downstream processing. Event type normalization is the responsibility of the emitter adaptor (openHIM mediator layer).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Build](#build)
- [Run](#run)
- [API Documentation](#api-documentation)
- [Rejected Event Management](#rejected-event-management)
- [Configuration Reference](#configuration-reference)
- [Monitoring & Observability](#monitoring--observability)
- [Architecture Decisions](#architecture-decisions)
- [Documentation](#documentation)

---

## Architecture Overview

```
┌───────────────────┐     POST /v1/events     ┌─────────────────────┐     cce.events.inbound     ┌───────────────┐
│  EHR / RHIE       │ ──────────────────────► │  Collector Service   │ ────────────────────────► │  Kafka Broker  │
│  (via openHIM)    │  CloudEvents v1.0 JSON  │                     │   Synchronous publish     │               │
└───────────────────┘                         │  ┌───────────────┐  │                           └───────────────┘
                                              │  │ PostgreSQL    │  │
                                              │  │ inbound_event_log │  │
                                              │  └───────────────┘  │
                                              └─────────────────────┘
```

**Processing Pipeline (8 steps):**
1. Receive HTTP POST and parse CloudEvents JSON body
2. Validate CloudEvents envelope (specversion, id, source, type, subject, data, datacontenttype)
3. Check payload size (default max: 1 MB)
4. Deduplication check (cloudevents_id + source within lookback window)
5. Persist to `inbound_event_log` table (status = RECEIVED)
6. Apply server-side defaults (correlationid, time)
7. Validate payload (FHIR R4 structural validation or plain JSON check)
8. Update status to ACCEPTED and publish to Kafka synchronously

## Technology Stack

| Concern | Technology | Version |
|---------|------------|---------|
| Language | Java | 21 (LTS) |
| Framework | Spring Boot | 3.4.x |
| Build tool | Gradle | 8.x |
| Database | PostgreSQL | 16+ |
| Message broker | Apache Kafka | 3.7+ (KRaft mode) |
| FHIR library | HAPI FHIR | 7.4.0 |
| Shared library | `cce-common-util` | 2.0.0 (sibling checkout, `includeBuild`) |
| Metrics | Micrometer + Prometheus | — |
| Logging | Logback + LogstashEncoder | JSON in production |

## Prerequisites

- **Java 21** (Eclipse Temurin recommended)
- **Docker** and **Docker Compose** (for local infrastructure)
- **Gradle 8.x** (wrapper included — no global install needed)
- **`cce-common-util`** checked out as a sibling directory — wired in via `includeBuild`, so there is
  nothing to publish or install; a change there is picked up on the next build

### What comes from cce-common-util

This service takes three things from the shared library, imported by name in `CommonUtilConfig`:

| From the library | Why it is not local |
|---|---|
| `FhirConfig` | The `FhirContext` is expensive to build and thread-safe once built, and every CCE service has to parse against the same R4 context |
| `ClinicalEventTimeExtractor` | The clinical time stamped on `inbound_event_log.event_time` is the same reading the Matcher Service later judges an SLA against. Held separately, the two drifted once already — an `Encounter` with both bounds was recorded here at `period.end` and judged there at `period.start` — and were reconciled by hand; sharing the class is what stops that recurring |
| `KafkaTopicProperties` | Names the topic this service produces to and the Matcher Service consumes from, so the two cannot end up on different spellings of one topic |

Imported by name rather than by scanning the whole library: this service owns one table and has no
business reaching for the runtime plane's entities and repositories.

## Quick Start

```bash
# 1. Start infrastructure (PostgreSQL + Kafka)
docker compose up -d

# 2. Wait for containers to be healthy
docker compose ps

# 3. Build the application
./gradlew build

# 4. Run with local profile
./gradlew bootRun --args='--spring.profiles.active=local'
```

The service starts on **port 8081** (local profile). Health check: `http://localhost:8081/actuator/health`

## Build

```bash
# Full build (compile + test)
./gradlew build

# Build without tests (faster, for deployment)
./gradlew build -x test

# Run tests only
./gradlew test

# Clean build
./gradlew clean build
```

The output JAR is at `build/libs/cce-collector-service-1.0.0-SNAPSHOT.jar`.

## Run

### Option 1: Docker Compose (full stack)

```bash
docker compose up -d
./gradlew bootRun --args='--spring.profiles.active=local'
```

### Option 2: Standalone JAR

```bash
java -jar build/libs/cce-collector-service-*.jar \
  --spring.profiles.active=production \
  --DB_HOST=db.example.com \
  --KAFKA_BOOTSTRAP=kafka.example.com:9092
```

### Option 3: Docker image

```bash
# Run from the directory holding both cce-collector-service and cce-common-util
docker build -f cce-collector-service/Dockerfile -t cce-collector-service .
docker run -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=production \
  -e DB_HOST=db.example.com \
  -e KAFKA_BOOTSTRAP=kafka.example.com:9092 \
  cce-collector-service
```

## API Documentation

### POST /v1/events — Ingest a CloudEvents event

**Request:**
```bash
curl -X POST http://localhost:8081/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "specversion": "1.0",
    "id": "evt-12345",
    "source": "ehr.facility-a.openhim",
    "type": "org.openhie.fhir.Encounter",
    "subject": "UPID-12345",
    "datacontenttype": "application/fhir+json",
    "data": {
      "resourceType": "Encounter",
      "id": "enc-001",
      "status": "finished",
      "class": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        "code": "AMB"
      },
      "subject": {
        "reference": "Patient/UPID-12345"
      }
    }
  }'
```

**Success Response (202 Accepted):**
```json
{
  "data": {
    "eventId": "01952c7a-...",
    "cloudEventsId": "evt-12345",
    "status": "accepted",
    "correlationId": "corr-a1b2c3d4-...",
    "receivedAt": "2026-03-11T10:30:00.000Z"
  }
}
```

**Duplicate Response (200 OK):**
```json
{
  "data": {
    "eventId": "01952c7a-...",
    "status": "duplicate"
  }
}
```

**Validation Error (400 Bad Request):**
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "specversion is required; subject is required"
  }
}
```

**Payload Validation Error (422 Unprocessable Entity):**
```json
{
  "error": {
    "code": "PAYLOAD_VALIDATION_ERROR",
    "message": "Payload validation failed: Failed to parse FHIR resource: ..."
  }
}
```

**Kafka Publish Failure (500 Internal Server Error):**
```json
{
  "error": {
    "code": "EVENT_PUBLISH_FAILURE",
    "message": "Event could not be published. Please retry."
  }
}
```

### Plain JSON Example (non-FHIR)

```bash
curl -X POST http://localhost:8081/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "specversion": "1.0",
    "id": "evt-plain-001",
    "source": "custom.source",
    "type": "com.example.CustomEvent",
    "subject": "UPID-67890",
    "datacontenttype": "application/json",
    "data": {
      "metric": "heart_rate",
      "value": 72,
      "unit": "bpm"
    }
  }'
```

## Rejected Event Management

Rejected events are tracked directly in the `inbound_event_log` table with `status = 'REJECTED'`. There are no separate REST endpoints for rejection management — investigation is done via direct database queries.

**Query rejected events:**
```sql
SELECT id, cloudevents_id, source, type, subject, rejection_reason, error_details, received_at
FROM inbound_event_log
WHERE status = 'REJECTED'
ORDER BY received_at DESC
LIMIT 20;
```

**Count rejected events by reason:**
```sql
SELECT rejection_reason, COUNT(*) as count
FROM inbound_event_log
WHERE status = 'REJECTED'
GROUP BY rejection_reason
ORDER BY count DESC;
```

**Investigate a specific rejected event:**
```sql
SELECT * FROM inbound_event_log WHERE id = '<uuid>';
```

## Configuration Reference

### Base Properties (`application.yml`)

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8080` | HTTP server port |
| `server.shutdown` | `graceful` | Graceful shutdown mode |
| `server.tomcat.threads.max` | `200` | Max Tomcat worker threads |
| `spring.lifecycle.timeout-per-shutdown-phase` | `30s` | Graceful shutdown timeout |
| `spring.datasource.url` | `jdbc:postgresql://localhost:5433/ccedb` | PostgreSQL JDBC URL |
| `spring.datasource.username` | `cce_user` | Database username |
| `spring.datasource.password` | `cce_pass` | Database password |
| `spring.datasource.hikari.maximum-pool-size` | `10` | HikariCP max pool size |
| `spring.datasource.hikari.minimum-idle` | `5` | HikariCP min idle connections |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka bootstrap servers |
| `spring.kafka.producer.acks` | `all` | Kafka producer acknowledgment |
| `spring.kafka.producer.retries` | `3` | Kafka producer retry count |
| `cce.collector.dedup.lookback-days` | `30` | Deduplication lookback window (days) |
| `cce.collector.max-payload-size` | `1048576` | Max payload size in bytes (1 MB) |
| `cce.collector.fhir.strict-validation` | `false` | FHIR strict validation mode |
| `cce.kafka.topics.inbound-events` | `cce.events.inbound` | Kafka inbound topic name — bound by cce-common-util's `KafkaTopicProperties`, the same key the Matcher Service reads |
| `cce.kafka.publish-timeout-seconds` | `30` | Kafka synchronous publish timeout |
| `cce.kafka.topic-config.partitions` | `25` | Inbound topic partition count |
| `cce.kafka.topic-config.replication-factor` | `1` | Inbound topic replication factor |
| `cce.kafka.topic-config.retention-ms` | `604800000` | Inbound topic retention (ms, 7 days) |
| `cce.kafka.topic-config.cleanup-policy` | `delete` | Inbound topic cleanup policy |
| `cce.kafka.topic-config.min-insync-replicas` | `1` | Inbound topic min in-sync replicas |

### Environment Variables

All properties can be overridden via environment variables:

| Variable | Maps To | Used In |
|----------|---------|---------|
| `SERVER_PORT` | `server.port` | All profiles |
| `DB_HOST` | DataSource URL host | local, staging, production |
| `DB_PORT` | DataSource URL port | local, staging, production |
| `DB_NAME` | DataSource URL database | local, staging, production |
| `DB_USERNAME` | `spring.datasource.username` | local, staging, production |
| `DB_PASSWORD` | `spring.datasource.password` | local, staging, production |
| `KAFKA_BOOTSTRAP` | `spring.kafka.bootstrap-servers` | All profiles |
| `KAFKA_BUFFER_MEMORY` | `spring.kafka.producer.properties.buffer.memory` | production |
| `CCE_COLLECTOR_KAFKA_TOPIC_PARTITIONS` | `cce.kafka.topic-config.partitions` | All profiles |
| `CCE_COLLECTOR_KAFKA_TOPIC_REPLICATION_FACTOR` | `cce.kafka.topic-config.replication-factor` | All profiles |
| `CCE_COLLECTOR_KAFKA_TOPIC_RETENTION_MS` | `cce.kafka.topic-config.retention-ms` | All profiles |
| `CCE_COLLECTOR_KAFKA_TOPIC_CLEANUP_POLICY` | `cce.kafka.topic-config.cleanup-policy` | All profiles |
| `CCE_COLLECTOR_KAFKA_TOPIC_MIN_INSYNC_REPLICAS` | `cce.kafka.topic-config.min-insync-replicas` | All profiles |
| `TOMCAT_THREADS_MAX` | `server.tomcat.threads.max` | staging, production |
| `HIKARI_MAX_POOL_SIZE` | `spring.datasource.hikari.maximum-pool-size` | staging, production |
| `HIKARI_MIN_IDLE` | `spring.datasource.hikari.minimum-idle` | staging, production |
| `CCE_DEDUP_LOOKBACK_DAYS` | `cce.collector.dedup.lookback-days` | All profiles |
| `CCE_COLLECTOR_MAX_PAYLOAD_SIZE` | `cce.collector.max-payload-size` | production |
| `CCE_KAFKA_PUBLISH_TIMEOUT_SECONDS` | `cce.kafka.publish-timeout-seconds` | staging, production |

### Profile-Specific Overrides

| Setting | local | staging | production |
|---------|-------|---------|------------|
| Server port | 8081 | 8080 | 8080 |
| HikariCP max pool | 5 | 15 | 20 |
| HikariCP min idle | 2 | 5 | 10 |
| Tomcat threads | 200 (base) | 100 | 200 |
| Kafka publish timeout | 30s (base) | 30s | 15s |
| Log level (service) | DEBUG | INFO | INFO |
| Log format | Console | Console | JSON (LogstashEncoder) |
| Health details | always | authorized | never |
| Hibernate SQL | shown | hidden | hidden |

## Monitoring & Observability

### Health Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /actuator/health` | Overall health (DB + Kafka) |
| `GET /actuator/health/liveness` | Kubernetes liveness probe |
| `GET /actuator/health/readiness` | Kubernetes readiness probe |

### Prometheus Metrics

```
GET /actuator/prometheus
```

**Key custom metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `cce_collector_events_received_total` | Counter | Total events received at POST /v1/events |
| `cce_collector_events_accepted_total` | Counter | Events that passed all validation |
| `cce_collector_events_rejected_total` | Counter | Validation failures (tagged by `reason`) |
| `cce_collector_events_duplicate_total` | Counter | Duplicate events detected |
| `cce_collector_kafka_publish_success_total` | Counter | Successful Kafka publishes |
| `cce_collector_kafka_publish_failure_total` | Counter | Failed Kafka publishes |
| `cce_collector_ingestion_duration_seconds` | Timer | Full pipeline duration |
| `cce_collector_fhir_validation_duration_seconds` | Timer | FHIR validation time |

### Logging

- **Local/Staging/Test:** Console output with MDC fields (`requestId`, `correlationId`, `cloudEventsId`, `source`, `subject`)
- **Production:** JSON structured logging via LogstashEncoder

## Architecture Decisions

1. **Single-table design:** All events (received, accepted, rejected) stored in `inbound_event_log`. No separate dead-letter or event-log tables. Simplifies operations and avoids cross-table consistency issues.

2. **Synchronous Kafka publish:** Events are published to Kafka within the HTTP request. No outbox pattern, no scheduled retry. Source systems retry on 500 errors. This keeps the service stateless and operationally simple.

3. **UUIDv7 primary keys:** Time-ordered UUIDs for efficient B-tree inserts and natural chronological ordering without a separate sequence.

4. **Idempotent POST:** Duplicate submissions return 200 OK (not 409 Conflict) with the existing event ID. Deduplication uses a configurable lookback window backed by a unique constraint.

5. **No event type normalization:** The Collector passes the `type` field through unchanged. Normalization is the responsibility of the emitter adaptor (openHIM mediator layer).

6. **Lowercase CloudEvents fields end-to-end:** HTTP request → database → Kafka — no camelCase translation at any boundary.

## Documentation

Detailed documentation is available in the [`docs/`](docs/) directory:

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Service responsibilities, component design, and position within the CCE platform |
| [API Reference](docs/api-reference.md) | REST endpoints, request/response schemas, and status codes |
| [Flow Diagrams](docs/flow-diagrams.md) | Mermaid diagrams of the event ingestion, deduplication, and publishing pipelines |
| [Kafka Events](docs/kafka-events.md) | Topic definitions, message schemas, partitioning strategy, and sample payloads |
| [Data Dictionary](docs/data-dictionary.md) | Database tables, columns, enums, CloudEvents fields, and Kafka message fields |
| [Deployment Guide](docs/deployment-guide.md) | Local setup, Docker Compose, building from source, and configuration reference |
| [Operations Runbook](docs/operations-runbook.md) | Health checks, Kubernetes probes, monitoring, alerting, and troubleshooting |
