# CCE Collector Service — Architecture

## 1. Purpose

The Collector Service is the **single point of entry** for all clinical events into the Care Coordination Engine (CCE) platform. No external system publishes directly to Kafka — every event flows through the Collector.

It receives clinical events from external EHR/RHIE systems (via openHIM mediators or direct integrations), validates them as CloudEvents v1.0 envelopes with FHIR R4 or plain JSON payloads, and publishes them to Kafka for downstream processing by the Compliance Service.

## 2. Responsibilities

| # | Responsibility | Description |
|---|---------------|-------------|
| 1 | **Receive clinical events** | REST API ingestion from openHIM RHIE mediators, EMR direct push, CHW apps |
| 2 | **Validate CloudEvents envelope** | Mandatory fields (`specversion`, `id`, `source`, `type`, `subject`), extensions, structure |
| 3 | **Validate FHIR R4 payloads** | Structural validation of `data` when `datacontenttype` = `application/fhir+json` |
| 4 | **Validate `type` presence** | Ensure `type` field is present and non-empty (mandatory CloudEvents attribute) — no format restriction |
| 5 | **Apply server-side defaults** | Generate `correlationid` (UUID with `corr-` prefix) if absent; fill `time` with server `received_at` if absent; derive `event_time` (clinical occurrence time) from the FHIR payload, falling back to envelope `time` then `received_at` |
| 6 | **Deduplicate inbound events** | Reject/mark duplicates using `(id, source)` compound key via PostgreSQL with configurable lookback window |
| 7 | **Publish to Kafka** | Topic `cce.events.inbound` with `subject` (patient_id) as partition key |
| 8 | **Record rejection details** | Persist rejection reason and error details on `inbound_event_log` for rejected events |
| 9 | **Health/readiness endpoints** | For Kubernetes orchestration |

### Explicit Exclusions

- Protocol matching, step completion, or compliance tracking → **Compliance Service**
- Time-based state transitions → **Scheduler Service**
- OAuth token management, routing, rate limiting → **Gateway Service**
- Analytics or reporting → **Analytics Service**
- Event type format validation or normalization (Collector only checks `type` is present and non-empty)
- FHIR profile conformance validation (structural parse only)
- Event transformation or enrichment beyond server-side defaults (`correlationid`, `time`, `event_time`)
- Event routing to multiple topics

## 3. System Context

```
┌─────────────────────────────────────────────────────────────┐
│                    External Systems                          │
│  eBUZIMA EMR  │  SmartCare  │  CHW App  │  Lab Systems      │
└──────┬────────┴──────┬──────┴─────┬─────┴──────┬────────────┘
       │               │            │            │
       ▼               ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│               openHIM / RHIE Mediator Layer                  │
│         (routes, transforms, adds correlation IDs)           │
└──────────────────────────┬──────────────────────────────────┘
                           │  HTTP POST (CloudEvents)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              ★ CCE Collector Service ★                        │
│    Validate → Deduplicate → Publish to Kafka                 │
└──────────────────────────┬──────────────────────────────────┘
                           │  Kafka: cce.events.inbound
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              CCE Compliance Service                           │
│    Match → Enroll → Complete Steps → Detect Deviations       │
└─────────────────────────────────────────────────────────────┘
```

## 4. Technology Stack

| Concern | Technology | Version |
|---------|------------|---------|
| Language | Java | 21 (LTS) |
| Framework | Spring Boot | 3.4.x |
| Build tool | Gradle | 8.x |
| Database | PostgreSQL | 16+ |
| Message broker | Apache Kafka | 3.7+ (KRaft mode) |
| FHIR library | HAPI FHIR | 7.4.0 |
| DB access | Spring Data JPA + Hibernate | (Spring Boot managed) |
| DB migration | Flyway | (Spring Boot managed) |
| Connection pool | HikariCP | (Spring Boot default) |
| Observability | Micrometer + Prometheus | (Spring Boot managed) |
| Testing | JUnit 5, Testcontainers, MockMvc | |

## 5. Package Structure

```
org.openphc.cce.collector/
├── CollectorServiceApplication.java       # Spring Boot entry point
├── config/                                # Spring configuration beans
│   ├── KafkaPublishProperties.java        #   @ConfigurationProperties for cce.kafka.* publish timeout
│   │                                      #   & topic creation config (the topic name is shared)
│   ├── KafkaTopicConfig.java              #   Declares NewTopic bean for auto-creation on startup
│   ├── CommonUtilConfig.java              #   Imports FhirConfig, ClinicalEventTimeExtractor,
│   │                                      #   KafkaTopicProperties from cce-common-util
│   ├── JpaConfig.java                     #   Enables JPA repositories & transactions
│   ├── SecurityConfig.java                #   Stateless security, CSRF disabled
│   ├── WebConfig.java                     #   CORS configuration
│   ├── KafkaHealthIndicator.java          #   Custom health indicator for Kafka broker connectivity
│   └── RequestLoggingFilter.java          #   OncePerRequestFilter: sets MDC requestId per request
├── domain/
│   ├── model/                             # JPA entities
│   │   ├── InboundEventLog.java              #   Raw inbound request record (audit/dedup/rejection tracking)
│   │   └── enums/
│   │       ├── InboundStatus.java         #   RECEIVED, ACCEPTED, REJECTED, DUPLICATE
│   │       └── RejectionReason.java       #   Validation/processing failure reasons
│   └── repository/                        # Spring Data JPA repositories
│       └── InboundEventLogRepository.java
├── service/                               # Core business logic
│   ├── EventIngestionService.java         #   Main orchestrator: validate → persist → publish
│   ├── CloudEventValidator.java           #   CloudEvents v1.0 envelope validation
│   ├── PayloadValidator.java              #   Payload validation: branches on datacontenttype (FHIR / JSON / unsupported)
│   ├── EventDefaultsEnricher.java         #   Generates correlationId if absent, fills time if absent, derives event_time
│   ├── EventPublisher.java                #   Publishes enriched EventIngestionRequest to Kafka
│   ├── DeduplicationService.java          #   DB dedup with configurable lookback window
│   └── RejectionService.java              #   Updates inbound_event_log with rejection details
├── kafka/
│   └── InboundEventProducer.java          # Kafka publish to cce.events.inbound
├── api/
│   ├── controller/
│   │   └── EventIngestionController.java  #   POST /v1/events
│   ├── dto/                               # Request/response data transfer objects
│   │   ├── ApiResponse.java               #   { "data": EventIngestionResponse } envelope
│   │   ├── ApiError.java                  #   { "error": { "code", "message" } }
│   │   ├── EventIngestionRequest.java     #   CloudEvents envelope (inbound DTO & Kafka message)
│   │   └── EventIngestionResponse.java    #   Accepted/rejected receipt
│   └── exception/
│       ├── GlobalExceptionHandler.java    #   @ControllerAdvice centralized error handling
│       ├── CloudEventValidationException.java
│       ├── PayloadValidationException.java
│       ├── KafkaPublishException.java
│       ├── DuplicateEventException.java
│       └── PatientIdNotFoundException.java
└── fhir/
    ├── FhirResourceParser.java            # HAPI FHIR parse (type detection: common-util)
    ├── FhirResourceValidator.java         # Structural validation + subject cross-check
    └── PatientIdExtractor.java            # Extracts patient UPID from FHIR resource (Patient identifier[], subject/patient reference)

src/main/resources/
├── application.yml                        # Common config (datasource, kafka, actuator, metrics)
├── application-local.yml                  # Local dev overrides (debug logging, relaxed pool sizes)
├── application-staging.yml                # Staging overrides
├── application-production.yml             # Production overrides
├── logback-spring.xml                     # Structured logging: JSON (production), console (others), MDC fields
└── db/migration/                          # Flyway migration scripts
```

## 6. Core Processing Algorithm

### Single Event Ingestion Flow

```
 1. Receive HTTP POST → parse request body
 2. CloudEvents Envelope Validation
    a. Required fields: specversion, id, source, type, subject, data
    b. specversion must be "1.0"
    c. subject must be non-empty (patient UPID required by CCE)
    d. If validation fails → 400 + update inbound_event_log status = 'REJECTED' with rejection details
 3. Deduplication Check
    a. Query PostgreSQL: check if (source, cloudevents_id) exists within lookback window
       - If exists → update status = 'DUPLICATE', return 200 (idempotent)
    b. If not found → proceed (DB unique constraint is authoritative)
 4. Persist to inbound_event_log (status = 'RECEIVED', raw_payload = original body)
 5. Apply Server-Side Defaults
    a. Generate correlationid if absent (UUID with "corr-" prefix)
    b. Fill time with server received_at if absent
    c. Derive event_time (clinical occurrence time) from FHIR payload via
       ClinicalEventTimeExtractor (cce-common-util); fall back to envelope time, then received_at
 7. Payload Validation
    a. If datacontenttype = application/fhir+json:
       i.   Parse data via HAPI FHIR
       ii.  Validate resourceType is present and a recognized FHIR R4 resource type
       iii. Cross-check patient UPID against envelope `subject` (reject on mismatch)
            - Patient resource: extracted from identifier[] matching configured system URI,
              fallback to Patient.id
            - RelatedPerson: extracted from patient reference
            - Other resources: extracted from subject or patient reference
       iv.  If invalid → status = 'REJECTED', rejection_reason = INVALID_FHIR, return 422
    b. If datacontenttype = application/json (non-FHIR):
       i.   Validate data is valid JSON
       ii.  If invalid → status = 'REJECTED', rejection_reason = INVALID_JSON, return 422
       iii. No FHIR-specific validation is performed
    c. Other datacontenttype values → status = 'REJECTED', rejection_reason = UNSUPPORTED_CONTENT_TYPE, return 400
 8. Update inbound_event_log.status = 'ACCEPTED'
 9. Publish to Kafka synchronously
    a. Key = subject (patient_id) — per-patient ordering
    b. On success: return HTTP 202 Accepted with ingestion receipt
    c. On failure: update inbound_event_log status = 'REJECTED', rejection_reason = KAFKA_PUBLISH_FAILURE, return HTTP 500
       Source system (openHIM) will retry based on its retry policy
```

## 7. Database Schema

One table owned by this service, managed by Flyway:

| Table | Purpose | Partitioned |
|-------|---------|-------------|
| `inbound_event_log` | Raw request audit log + rejection tracking; primary dedup via `UNIQUE(cloudevents_id, source)` | No |

### Entity

```
inbound_event_log  (single table — audit, dedup, rejection tracking)
```

> **Note:** Rejected events are tracked directly on `inbound_event_log` via `status`, `rejection_reason`, and `error_details` columns. 

### Deduplication Constraints & Indexes

| Constraint / Index | Table | Columns | Purpose |
|-----------|-------|---------|--------|
| `UNIQUE(cloudevents_id, source)` | `inbound_event_log` | `(cloudevents_id, source)` | Authoritative dedup (unique constraint) |
| `idx_inbound_event_log_dedup` | `inbound_event_log` | `(cloudevents_id, source, received_at)` | Lookback dedup query — covers `WHERE cloudevents_id = ? AND source = ? AND received_at > ?` (index-only scan) |


## 8. Deduplication Strategy

PostgreSQL-based deduplication with a configurable lookback window (default: 30 days) + unique constraints (permanent).

### Lookback Query

On event arrival, the service queries `inbound_event_log` for records matching `(source, cloudevents_id)` within the configured lookback window. This limits the query scope instead of scanning the entire database.

### PostgreSQL Unique Constraint (Authoritative)

The unique constraint on `inbound_event_log` serves as the permanent deduplication layer.

### Idempotency Contract

- Same event submitted twice → **200 OK** with `status: "duplicate"` (not 409)
- Event is **not** re-published to Kafka on duplicate
- Standard idempotent POST pattern used by openHIM mediators

## 9. Kafka Integration

### Topics Produced

| Topic | Key | Purpose |
|-------|-----|---------|
| `cce.events.inbound` | `subject` (patient UPID) | Validated events for Compliance Service |

> **Note:** Rejected events are tracked in `inbound_event_log` (status = REJECTED, rejection_reason, error_details). No dead-letter topic is implemented — rejection monitoring is done via database queries and the `cce.collector.events.rejected` counter metric (tagged by reason).

### Producer Configuration

All producer settings are env-var-configurable via `application.yml` (see Deployment Guide for full env var reference).

| Setting | Default | Env Var | Rationale |
|---------|---------|---------|--------|
| `acks` | `all` | `SPRING_KAFKA_PRODUCER_ACKS` | Wait for all in-sync replicas |
| `retries` | `3` | `SPRING_KAFKA_PRODUCER_RETRIES` | Retry on transient failures (production: 5) |
| `enable.idempotence` | `true` | — | Exactly-once within a partition |
| `linger.ms` | `5` | `KAFKA_LINGER_MS` | Small batching window (production: 10) |
| `batch.size` | `16384` | `KAFKA_BATCH_SIZE` | 16 KB batch size (production: 32 KB) |
| `buffer.memory` | `33554432` | `KAFKA_BUFFER_MEMORY` | 32 MB producer buffer |

### Topic Auto-Creation

`KafkaTopicConfig` declares a `NewTopic` bean that creates the inbound topic on startup (if it doesn't already exist), under the name cce-common-util's `KafkaTopicProperties` supplies (`cce.kafka.topics.inbound-events`). The creation settings are local, in `KafkaPublishProperties.TopicConfig` (`cce.kafka.topic-config.*`) — no other service creates this topic:

| Setting | Default | Env Var | Production Override |
|---------|---------|---------|--------------------|
| `partitions` | `25` | `CCE_COLLECTOR_KAFKA_TOPIC_PARTITIONS` | 25 |
| `replication-factor` | `1` | `CCE_COLLECTOR_KAFKA_TOPIC_REPLICATION_FACTOR` | 3 |
| `retention-ms` | `604800000` (7 days) | `CCE_COLLECTOR_KAFKA_TOPIC_RETENTION_MS` | 604800000 |
| `cleanup-policy` | `delete` | `CCE_COLLECTOR_KAFKA_TOPIC_CLEANUP_POLICY` | delete |
| `min-insync-replicas` | `1` | `CCE_COLLECTOR_KAFKA_TOPIC_MIN_INSYNC_REPLICAS` | 2 |

> **Note:** If the topic already exists, the broker will not alter partitions or replication factor. To change partitions on an existing topic, use `kafka-topics.sh --alter`.

### Kafka Publish

Kafka publish is **synchronous** within the HTTP request. On failure, the Collector returns HTTP 500 and marks the event as `REJECTED` with `KAFKA_PUBLISH_FAILURE`. The source system (openHIM) is expected to retry.

> **No outbox pattern.** There is no scheduled retry for failed Kafka publishes. The Collector relies on the source system's retry behaviour.

## 10. Payload Handling

The Collector supports two `datacontenttype` values, determining the level of payload validation applied:

### `application/fhir+json`

FHIR R4 structural validation via HAPI FHIR.

| Check | Level | Action on Failure |
|-------|-------|-------------------|
| `data` parses as valid JSON | Required | Reject (`INVALID_FHIR`) |
| `data.resourceType` present and a recognized FHIR R4 resource type | Required | Reject (`INVALID_FHIR`) |
| HAPI FHIR can parse into `IBaseResource` | Required | Reject (`INVALID_FHIR`) |
| Patient UPID matches `subject` | Required | Reject (`INVALID_FHIR`) |

**Patient UPID Extraction Strategy:**

| Resource Type | Extraction Method |
|---------------|-------------------|
| `Patient` | From `identifier[]` matching configured system URI (`http://openphc.org/identifier/upid`), fallback to `Patient.id` |
| `RelatedPerson` | From `patient` reference (`Patient/<UPID>`) |
| All other resources | From `subject` or `patient` reference (`Patient/<UPID>`) |

### `application/json` (non-FHIR)

Non-FHIR JSON payloads are also supported per the CCE solution design. Only basic JSON validity is checked.

| Check | Level | Action on Failure |
|-------|-------|-------------------|
| `data` parses as valid JSON | Required | Reject (`INVALID_JSON`) |
| `data` is a non-empty JSON object | Required | Reject (`INVALID_JSON`) |

No FHIR-specific validation (resourceType, HAPI parsing, subject cross-check) is performed for non-FHIR payloads.

### Unsupported content types

Any `datacontenttype` other than `application/fhir+json` or `application/json` is rejected with `UNSUPPORTED_CONTENT_TYPE`.

### `type` Field

The `type` field is a mandatory CloudEvents v1.0 attribute. The Collector validates only that it is **present and non-empty** — it does not enforce any specific format or pattern. The emitter adaptor (openHIM mediator) sets the value; the Collector passes it through to Kafka unchanged.

> **Tier 1 structural matching** in the Compliance Service uses `data.resourceType` (payload), not the envelope `type`.

## 11. Compliance Service Contract

The Compliance Service consumes CloudEvents JSON objects from `cce.events.inbound` with these guarantees from the Collector:

1. **`subject` is always present** — used for patient protocol instance lookup
2. **`type` is always present and non-empty** — passed through from the emitter; Compliance Service uses `data.resourceType` (not `type`) for Tier 1 structural matching
3. **`data` contains a valid payload** — FHIR R4 resource (parseable via HAPI FHIR) when `datacontenttype` is `application/fhir+json`; valid JSON object when `datacontenttype` is `application/json`
4. **Field names use CloudEvents spec convention (lowercase)** — e.g., `specversion`, `datacontenttype`, `correlationid`
5. **Kafka key is `subject`** — per-patient ordering
6. **`correlationid` is always present** — for distributed tracing
7. **Each message maps to an `inbound_event_log` row** — authoritative source of truth

The Collector does NOT populate `protocolinstanceid`, `protocoldefinitionid`, or `actionid` — the Compliance Service resolves these independently.
