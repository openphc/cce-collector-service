# CCE Collector Service — Release Notes

> Entries before 2.0.0 name the **Compliance Service** as the consumer of `cce.events.inbound`. That
> was accurate at the time: in 1.x one service consumed the topic and owned everything downstream of
> ingestion. In 2.0.0 that consumer is the **Matcher Service**, and step SLA verification is the
> **Step SLA Service** (`cce-step-sla-service`). Past entries are left as written, since a changelog
> that is edited to match the present stops being a record of what shipped.

---

## Unreleased — 2.0.0: adopting cce-common-util

**Branch:** `release-2.0.0`

This service now builds on the shared library, like the other four CCE services. Three things it had
grown its own copies of come from cce-common-util instead:

- **`FhirConfig`** — was identical to the library's, bean for bean.
- **`ClinicalEventTimeExtractor`** — the copies were reconciled by hand earlier on this branch, after
  the Matcher Service and this service were found to disagree about whether an `Encounter` occurred at
  `period.start` or `period.end`. Since this service stamps `event_time` and the matcher derives
  `completed_at` — and so an SLA verdict — from the same payload, the audit trail could not be used to
  explain the SLA clock. Sharing one class is what keeps them reconciled. No behaviour change: the
  candidate table is the one already on this branch. The two `cce.clinical_time.*` counters now carry
  Micrometer descriptions, which surface as HELP text in the Prometheus scrape.
- **`KafkaTopicProperties`** — named the topic under `cce.kafka.topics.inbound` while the matcher read
  `cce.kafka.topics.inbound-events`. One topic, two spellings, and a deployment had to set both.
  **Config change:** `cce.kafka.topics.inbound` → `cce.kafka.topics.inbound-events`, and
  `CCE_COLLECTOR_KAFKA_TOPICS_INBOUND` → `CCE_KAFKA_TOPICS_INBOUND_EVENTS`. The publish timeout and
  topic-creation settings stay local, in `KafkaPublishProperties` — no other service creates this topic.

The library is imported bean by bean in `CommonUtilConfig` rather than by widening `scanBasePackages`:
this service owns one table and has no business holding the runtime plane's entities and repositories.
The imports sit in a scanned `@Configuration` rather than on the application class, because an
`@Import` there is honoured by `@DataJpaTest` too, which would then have to supply a `MeterRegistry`.

---

## 1.0.0 — Clinical Event Time (`event_time`)

**Branch:** `release-1.0.0`

### Summary

Added an `event_time` column to `inbound_event_log` capturing the **clinical occurrence time** — when the clinical act actually happened — as distinct from `received_at` (ingestion time) and the CloudEvents envelope `time` (emitter transmission clock). This lets downstream scheduling (Compliance Service) key off real-world clinical time so ingestion lag does not shift due/overdue/missed dates.

- **New `ClinicalEventTimeExtractor`** (ported from [cce-compliance-service#64](https://github.com/openphc/cce-compliance-service/pull/64)) — best-effort extraction of the clinical time from the FHIR payload, with a per-resource-type candidate-field mapping (`Observation`, `Encounter`, `Procedure`, `Immunization`, `MedicationAdministration`, `Condition`, `ServiceRequest`, `DiagnosticReport`). See `docs/data-dictionary.md` §8.
- **`EventDefaultsEnricher`** now derives `event_time` during enrichment with precedence: clinical time → envelope `time` → `received_at`. Extraction never throws; failures fall back silently.
- **`event_time` is persistence-only** — not added to the Kafka message; consumers derive clinical time from the FHIR `data` payload themselves.
- New metrics: `cce.clinical_time.unmapped`, `cce.clinical_time.unparseable`.

### Migration

- `V2__add_event_time_to_inbound_event_log.sql` — adds nullable `event_time TIMESTAMPTZ` + `idx_inbound_event_log_event_time` index.

---

## v1.0.0 — Initial Release

**Branch:** `release-1.0.0`  
**Date:** 2026-03-12  
**Status:** Feature-complete, 199 tests passing  

---

## v1.0.0-patient — Patient & RelatedPerson Support

**Branch:** `release-1.0.0_patient`  
**Date:** 2026-03-13  
**Status:** Feature-complete, all tests passing  

### Summary

Extended `PatientIdExtractor` to properly handle `Patient` and `RelatedPerson` FHIR R4 resources. Previously, the extractor relied solely on reflection to find `getSubject()` or `getPatient()` methods, which failed for `Patient` resources (they have neither). This release adds:

- **Patient resource support**: Extracts UPID from `Patient.identifier[]` matching the configured system URI (`http://openphc.org/identifier/upid`), with fallback to `Patient.id`
- **Configurable identifier system**: New property `cce.collector.fhir.patient-identifier-system` (env: `CCE_COLLECTOR_FHIR_PATIENT_IDENTIFIER_SYSTEM`)
- **RelatedPerson**: Confirmed working via existing `getPatient()` reflection path

### New Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CCE_COLLECTOR_FHIR_PATIENT_IDENTIFIER_SYSTEM` | `http://openphc.org/identifier/upid` | System URI for matching Patient.identifier[] to extract UPID |

### Infrastructure Change

- PostgreSQL and Kafka extracted to shared infrastructure (`deploy-scripts/docker-compose.yml` with `cce-net` bridge network)
- Collector service `docker-compose.yml` now references external `deploy-scripts_cce-net` network

---

### Overview

First production-ready release of the CCE Collector Service — the single point of entry for all clinical events into the Care Coordination Engine (CCE) platform. The service receives CloudEvents v1.0 envelopes from external EHR/RHIE systems (via openHIM mediators or direct integrations), validates them, deduplicates, persists an audit trail, and publishes accepted events to Kafka for downstream processing by the Compliance Service.

---

### Features

#### Core Pipeline

| # | Feature | Ticket | Description |
|---|---------|--------|-------------|
| 1 | **REST Ingestion Endpoint** | CCE-81 | `POST /v1/events` — accepts CloudEvents JSON, returns `202 Accepted` or `200 OK` (duplicate) |
| 2 | **CloudEvents Envelope Validation** | CCE-52 | Validates `specversion` (must be `1.0`), `id`, `source`, `type`, `subject`, `datacontenttype`, `data` as required fields |
| 3 | **FHIR R4 Payload Validation** | CCE-54 | Structural validation via HAPI FHIR R4 when `datacontenttype` = `application/fhir+json`; subject cross-check (`Patient/<subject>` in `data.subject.reference`) |
| 4 | **JSON Payload Validation** | CCE-55 | Validates non-FHIR payloads when `datacontenttype` = `application/json` (must be valid, non-empty JSON) |
| 5 | **Deduplication** | CCE-56 | Compound key `(cloudevents_id, source)` with configurable lookback window (default 24h); returns idempotent `200 OK` for duplicates |
| 6 | **Server-Side Defaults** | CCE-81 | Auto-generates `correlationid` (`corr-<UUID>`) and `time` (server `received_at`) if absent |
| 7 | **Kafka Publishing** | CCE-58 | Synchronous publish to `cce.events.inbound` topic with `subject` as partition key; at-least-once delivery guarantee |
| 8 | **Rejection Tracking** | CCE-60 | Rejected events recorded on `inbound_event_log` table with `rejection_reason` and `error_details` (capped at 4000 chars) |

#### Observability

| # | Feature | Ticket | Description |
|---|---------|--------|-------------|
| 9 | **Micrometer Metrics** | CCE-61 | Custom counters, timers, and gauges for ingestion throughput, rejections (by reason), Kafka publish success/failure, and processing duration |
| 10 | **Prometheus Endpoint** | CCE-61 | `/actuator/prometheus` with all business and infrastructure metrics |
| 11 | **Custom Kafka Health Indicator** | CCE-61 | `KafkaHealthIndicator` verifies broker connectivity via `AdminClient.describeCluster()` |
| 12 | **Spring Actuator Health** | CCE-61 | `/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness` with DB and Kafka status |

#### Infrastructure

| # | Feature | Ticket | Description |
|---|---------|--------|-------------|
| 13 | **PostgreSQL Schema** | CCE-49 | `inbound_event_log` table with UUIDv7 primary keys, Flyway migration `V1__create_inbound_event_log.sql` |
| 14 | **Docker Compose** | CCE-49 | Collector service container; shared infra (PostgreSQL 16, Kafka 3.7.0 KRaft) managed via `deploy-scripts/docker-compose.yml` on `cce-net` network |
| 15 | **Profile Configuration** | CCE-63 | `local`, `staging`, `production` profiles with env-var-wrapped values for all tunable settings |
| 16 | **Graceful Shutdown** | CCE-63 | `server.shutdown: graceful` with 30s drain timeout |

---

### Tech Stack

| Component | Version / Technology |
|-----------|---------------------|
| Language | Java 21 (LTS) |
| Framework | Spring Boot 3.4.x |
| Build Tool | Gradle 8.x |
| Database | PostgreSQL 16 |
| Messaging | Apache Kafka 3.7.0 (KRaft) |
| FHIR | HAPI FHIR R4 (structural validation) |
| Observability | Micrometer + Prometheus |
| Migrations | Flyway |
| Containerization | Docker + Docker Compose |

---

### API Summary

| Method | Endpoint | Description | Success Codes |
|--------|----------|-------------|---------------|
| `POST` | `/v1/events` | Ingest a CloudEvents payload | `202 Accepted`, `200 OK` (duplicate) |
| `GET` | `/actuator/health` | Application health | `200 OK` |
| `GET` | `/actuator/health/liveness` | Liveness probe | `200 OK` |
| `GET` | `/actuator/health/readiness` | Readiness probe | `200 OK` |
| `GET` | `/actuator/prometheus` | Prometheus metrics scrape | `200 OK` |

---

### Kafka Topics

| Topic | Direction | Key | Value |
|-------|-----------|-----|-------|
| `cce.events.inbound` | Produced | `subject` (patient UPID) | CloudEvents JSON |

---

### Validation & Rejection Reasons

All rejected events are persisted to `inbound_event_log` with the appropriate status and reason:

| Rejection Reason | HTTP Code | Trigger |
|-----------------|-----------|---------|
| `INVALID_ENVELOPE` | 400 | Missing/invalid CloudEvents required fields |
| `INVALID_FHIR` | 400 | FHIR R4 structural validation failure |
| `INVALID_JSON` | 400 | Non-FHIR JSON payload is invalid or empty |
| `UNSUPPORTED_CONTENT_TYPE` | 400 | `datacontenttype` not `application/fhir+json` or `application/json` |
| `MISSING_SUBJECT` | 400 | `subject` field missing |
| `PAYLOAD_TOO_LARGE` | 400 | Request body exceeds max payload size (1 MB) |
| `DESERIALIZATION_ERROR` | 400 | Request body not parseable as JSON |
| `DUPLICATE` | 200 | Same `(id, source)` within lookback window |
| `KAFKA_PUBLISH_FAILURE` | 500 | Kafka broker unavailable or publish timeout |
| `INTERNAL_ERROR` | 500 | Unexpected failure (safety-net for orphaned RECEIVED records) |

---

### Metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `cce.collector.events.received` | Counter | — | Total events received |
| `cce.collector.events.accepted` | Counter | — | Events successfully published |
| `cce.collector.events.duplicate` | Counter | — | Duplicate events detected |
| `cce.collector.events.rejected` | Counter | `reason` | Rejected events by reason |
| `cce.collector.ingestion.duration` | Timer | — | End-to-end processing time |
| `cce.collector.kafka.publish.success` | Counter | — | Successful Kafka publishes |
| `cce.collector.kafka.publish.failure` | Counter | — | Failed Kafka publishes |

---

### Test Coverage

| Category | Tests | Description |
|----------|-------|-------------|
| DTO Serialization | 20 | Request/response JSON serialization, validation, edge cases |
| CloudEvents Validation | 19 | Envelope field validation, spec version, required fields |
| Payload Validation | 24 | FHIR R4 structural, JSON validation, content type routing |
| Event Defaults | 8 | Correlation ID generation, time enrichment, type pass-through |
| Deduplication | 6 | Duplicate detection, lookback window, unique constraint |
| Kafka Producer | 8 | Publish success/failure, partition key, timeout handling |
| Rejection Service | 10 | Error detail truncation (4000 char cap), rejection reason mapping |
| Exception Handling | 20 | Global handler, all exception types, response structure |
| Metrics | 12 | Counter/timer/gauge instrumentation, rejection reason tags |
| Health Indicators | 5 | Kafka/DB health, degraded states |
| Configuration | 6 | Properties binding, profile overrides |
| End-to-End Integration | 23 | Full pipeline (happy path, validation failures, Kafka, duplicates) |
| REST Controller | 10 | HTTP method routing, content negotiation, status codes |
| Service Orchestration | 28 | Ingestion flow, error paths, safety-net exception handling |
| **Total** | **199** | **All passing** |

---

### Database Schema

Single table `inbound_event_log` with indexes:

| Index | Columns | Purpose |
|-------|---------|---------|
| `uq_inbound_event_log_id_source` | `(cloudevents_id, source)` | Deduplication (unique constraint) |
| `idx_inbound_event_log_dedup` | `(cloudevents_id, source, received_at)` | Dedup lookback queries |
| `idx_inbound_event_log_source` | `source` | Source-filtered queries |
| `idx_inbound_event_log_status` | `status` | Status-based filtering |
| `idx_inbound_event_log_received` | `received_at` | Time-range queries |

---

### Configuration Highlights

All critical values are env-var-wrapped for runtime override without redeployment:

```yaml
server:
  port: ${SERVER_PORT:8080}
  shutdown: graceful

spring:
  datasource:
    hikari:
      maximum-pool-size: ${SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE:10}
  kafka:
    producer:
      retries: ${KAFKA_PRODUCER_RETRIES:3}
      properties:
        linger.ms: ${KAFKA_LINGER_MS:5}
        batch.size: ${KAFKA_BATCH_SIZE:16384}
        buffer.memory: ${KAFKA_BUFFER_MEMORY:33554432}

cce:
  kafka:
    topic: ${CCE_KAFKA_TOPIC:cce.events.inbound}
    publish-timeout: ${CCE_KAFKA_PUBLISH_TIMEOUT:10s}
    topic-config:
      partitions: ${CCE_COLLECTOR_KAFKA_TOPIC_PARTITIONS:25}
      replication-factor: ${CCE_COLLECTOR_KAFKA_TOPIC_REPLICATION_FACTOR:1}
      retention-ms: ${CCE_COLLECTOR_KAFKA_TOPIC_RETENTION_MS:604800000}
      cleanup-policy: ${CCE_COLLECTOR_KAFKA_TOPIC_CLEANUP_POLICY:delete}
      min-insync-replicas: ${CCE_COLLECTOR_KAFKA_TOPIC_MIN_INSYNC_REPLICAS:1}
  deduplication:
    lookback-hours: ${CCE_DEDUP_LOOKBACK_HOURS:24}
  validation:
    max-payload-size: ${CCE_VALIDATION_MAX_PAYLOAD_SIZE:1048576}
```

**Profiles:**

| Profile | Purpose | Key Overrides |
|---------|---------|---------------|
| `local` | Development | port 8081, pool 5/2, show-sql, DEBUG logging |
| `staging` | Pre-production | pool 15, Kafka INFO logging |
| `production` | Production | pool 20/10, tuned HikariCP & Kafka, WARN logging |

---

### Known Limitations

- **Single event ingestion only** — no batch endpoint (`POST /v1/events/batch`)
- **No dead-letter Kafka topic** — rejected events tracked in database only
- **No mTLS** — authentication/authorization deferred to Gateway Service
- **No rate limiting** — expected to be handled by openHIM / API gateway
- **No rejected event management endpoints** — query database directly or use Prometheus metrics
- **FHIR structural validation only** — no profile conformance or terminology validation
- **Patient UPID extraction** — requires `identifier.system` to match configured URI; resources without `subject`, `patient`, or matching identifier will be rejected
- **No event type format validation** — `type` field checked for presence only (no pattern enforcement)

---

### Breaking Changes

N/A — initial release.

---

### Migration Notes

1. **Database:** Flyway auto-applies `V1__create_inbound_event_log.sql` on first startup
2. **Kafka:** Topic `cce.events.inbound` is auto-created on startup by `KafkaTopicConfig` (25 partitions, RF 1 by default; production profile: RF 3, min-insync-replicas 2)
3. **Docker:** Start shared infrastructure (`deploy-scripts/docker-compose.yml`) first to create `cce-net` network, then start collector service (`docker compose up -d`)

---

### Documentation

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | System context, component diagram, data flow |
| [api-reference.md](api-reference.md) | REST endpoint specification with examples |
| [data-dictionary.md](data-dictionary.md) | Database schema, enums, field mappings |
| [kafka-events.md](kafka-events.md) | Kafka topic schema and publishing contracts |
| [flow-diagrams.md](flow-diagrams.md) | Mermaid sequence diagrams for all flows |
| [operations-runbook.md](operations-runbook.md) | Health checks, metrics, alerting, troubleshooting |
| [deployment-guide.md](deployment-guide.md) | Local setup, Docker, profile configuration |

---

### Contributors

| Ticket | Scope |
|--------|-------|
| CCE-53 | Service design documents |
| CCE-49 | Project init, JPA entities, Flyway migration |
| CCE-51 | Deduplication strategy and indexing |
| CCE-52 | CloudEvents DTOs and validation |
| CCE-54 | FHIR R4 payload validation |
| CCE-55 | JSON payload validation and exceptions |
| CCE-56 | Deduplication service |
| CCE-58 | Kafka producer (synchronous) |
| CCE-60 | Rejection tracking (refactored from standalone endpoints) |
| CCE-81 | Event ingestion orchestrator, REST endpoint, defaults enrichment |
| CCE-61 | Observability (Micrometer, Kafka health indicator) |
| CCE-62 | Integration tests (23 end-to-end tests) |
| CCE-63 | Configuration tuning, documentation, code optimization |

