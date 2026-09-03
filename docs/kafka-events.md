# CCE Collector Service — Kafka Events

Detailed reference for all Kafka topics, message schemas, publishing contracts, and sample payloads.

---

## 1. Topics

### 1.1 `cce.events.inbound` — Primary Event Topic

| Property | Value |
|----------|-------|
| **Topic** | `cce.events.inbound` |
| **Direction** | Produced by Collector, consumed by Matcher Service |
| **Message Key** | `subject` (patient UPID) — guarantees per-patient ordering. Extracted from FHIR resource: Patient `identifier[]` (matching system URI) or `id`; RelatedPerson `patient` reference; other resources via `subject`/`patient` reference |
| **Message Value** | CloudEvents JSON (lowercase field names per spec) |
| **Serialization** | Key: `StringSerializer`, Value: `JsonSerializer` |
| **Guarantees** | At-least-once delivery (idempotent producer, synchronous publish) |
| **Ordering** | Per-patient ordering within a partition (key = patient UPID) |

### 1.2 Rejected Event Handling

Rejected events are **not** published to a Kafka topic. They are tracked directly in the `inbound_event_log` database table with `status = 'REJECTED'`, `rejection_reason`, and `error_details` columns. This avoids a dependency on Kafka for monitoring rejections.

Operational monitoring of rejections is supported via:
- **Database queries** — see the Operations Runbook for SQL examples
- **Per-reason counter** — `cce.collector.events.rejected` with a `reason` tag

---

## 2. Producer Configuration

Kafka producer settings are defined in `application.yml` with environment variable overrides. Topic creation is handled by `KafkaTopicConfig`, a `@Configuration` class that declares a `NewTopic` bean for auto-creating the inbound topic on startup. Spring Boot auto-configuration is used for producer settings.

```yaml
spring:
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: ${SPRING_KAFKA_PRODUCER_ACKS:all}
      retries: ${SPRING_KAFKA_PRODUCER_RETRIES:3}
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        linger.ms: ${KAFKA_LINGER_MS:5}
        batch.size: ${KAFKA_BATCH_SIZE:16384}
        buffer.memory: ${KAFKA_BUFFER_MEMORY:33554432}
```

| Setting | Default | Env Var | Rationale |
|---------|---------|---------|--------|
| `acks` | `all` | `SPRING_KAFKA_PRODUCER_ACKS` | Wait for all in-sync replicas — no data loss |
| `retries` | `3` | `SPRING_KAFKA_PRODUCER_RETRIES` | Retry transient failures (production: 5) |
| `enable.idempotence` | `true` | — | Prevent duplicate messages on retry |
| `max.in.flight.requests.per.connection` | `5` | — | Max with idempotence enabled (Kafka requirement) |
| `linger.ms` | `5` | `KAFKA_LINGER_MS` | Slight batching delay for throughput (production: 10) |
| `batch.size` | `16384` | `KAFKA_BATCH_SIZE` | 16 KB batch size (production: 32 KB) |
| `buffer.memory` | `33554432` | `KAFKA_BUFFER_MEMORY` | 32 MB producer memory buffer |

### Topic Configuration

The topic **name** comes from cce-common-util's `KafkaTopicProperties`
(`@ConfigurationProperties(prefix = "cce.kafka.topics")`) — the same property the Matcher Service reads,
so producer and consumer cannot end up on different spellings of one topic. The publish timeout is this
service's own, in `KafkaPublishProperties` (`prefix = "cce.kafka"`):

| Property | Default | Env Var |
|----------|---------|--------|
| `cce.kafka.topics.inbound-events` | `cce.events.inbound` | `CCE_KAFKA_TOPICS_INBOUND_EVENTS` |
| `cce.kafka.publish-timeout-seconds` | `30` | `CCE_COLLECTOR_KAFKA_PUBLISH_TIMEOUT_SECONDS` |

### Topic Creation Configuration

Topic creation settings are managed via `KafkaPublishProperties.TopicConfig` (`cce.kafka.topic-config.*`) — local to this service, as no other service creates this topic. The `KafkaTopicConfig` class declares a `NewTopic` bean that creates the topic on startup if it doesn't exist, under the name the shared properties supply.

| Property | Default | Env Var | Production Override |
|----------|---------|--------|--------------------|
| `cce.kafka.topic-config.partitions` | `25` | `CCE_COLLECTOR_KAFKA_TOPIC_PARTITIONS` | 25 |
| `cce.kafka.topic-config.replication-factor` | `1` | `CCE_COLLECTOR_KAFKA_TOPIC_REPLICATION_FACTOR` | 3 |
| `cce.kafka.topic-config.retention-ms` | `604800000` (7 days) | `CCE_COLLECTOR_KAFKA_TOPIC_RETENTION_MS` | 604800000 |
| `cce.kafka.topic-config.cleanup-policy` | `delete` | `CCE_COLLECTOR_KAFKA_TOPIC_CLEANUP_POLICY` | delete |
| `cce.kafka.topic-config.min-insync-replicas` | `1` | `CCE_COLLECTOR_KAFKA_TOPIC_MIN_INSYNC_REPLICAS` | 2 |

> **Note:** If the topic already exists, the broker will not alter partitions or replication factor. To change partitions on an existing topic, use `kafka-topics.sh --alter`.

---

## 3. Kafka Message Schema

The Kafka message value is a CloudEvents JSON object. Field names use **lowercase** per the CloudEvents spec — no field name translation is performed by the Collector.

```json
{
  "id": "evt-eb010001-0001-4000-8000-000000000001",
  "source": "rhie-mediator",
  "type": "org.openphc.cce.encounter",
  "specversion": "1.0",
  "subject": "260225-0002-5501",
  "time": "2026-02-25T08:00:00Z",
  "datacontenttype": "application/fhir+json",
  "correlationid": "corr-1343872c-636d-506f-b041-1e571d426932",
  "sourceeventid": "enc-visit-20260225-0001",
  "protocolinstanceid": null,
  "protocoldefinitionid": null,
  "actionid": null,
  "facilityid": "0002",
  "data": {
    "resourceType": "Encounter",
    "id": "enc-uuid-visit-kicukiro-001",
    "status": "in-progress",
    "...": "..."
  }
}
```

### Field Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `String` | Yes | CloudEvents event identifier (from source) |
| `source` | `String` | Yes | Event source (e.g., `rhie-mediator`, `ebuzima/kigali-south`) |
| `type` | `String` | Yes | Event type (mandatory CloudEvents attribute, passed through from emitter) |
| `specversion` | `String` | Yes | Always `"1.0"` |
| `subject` | `String` | Yes | Patient UPID — also the Kafka message key |
| `time` | `ISO-8601 datetime` | Yes | Event time (source-provided or server-generated) |
| `datacontenttype` | `String` | Yes | MIME type (typically `application/fhir+json`) |
| `correlationid` | `String` | Yes | Distributed tracing ID (source-provided or generated `corr-<uuid>`) |
| `sourceeventid` | `String` | No | Source system's internal event ID |
| `protocolinstanceid` | `String` | No | Protocol instance UUID (usually null — Matcher Service resolves) |
| `protocoldefinitionid` | `String` | No | Protocol definition UUID (usually null — Matcher Service resolves) |
| `actionid` | `String` | No | Action/step ID (usually null — Matcher Service resolves) |
| `facilityid` | `String` | No | Healthcare facility FOSA ID |
| `data` | `Object` | Yes | FHIR R4 resource JSON or valid JSON object (structurally validated) |

> **Note:** `null` fields are omitted from the JSON output (`@JsonInclude(NON_NULL)`).

---

## 4. Synchronous Publish

The Collector publishes to Kafka **synchronously** during request processing. There is no outbox pattern or background retry.

```
HTTP Request → Validate → Persist (inbound_event_log, status=ACCEPTED) → Kafka Publish → 202 Accepted
                                                                     → Failure → Update (status=REJECTED, reason=KAFKA_PUBLISH_FAILURE) → 500 Internal Server Error
```

### How It Works

1. Validated event is persisted to `inbound_event_log` with `status = 'ACCEPTED'`
2. `KafkaTemplate.send()` publishes the message to Kafka synchronously
3. On success: HTTP 202 returned to caller
4. On failure: `inbound_event_log` updated to `status = 'REJECTED'`, `rejection_reason = 'KAFKA_PUBLISH_FAILURE'` — HTTP 500 returned to caller
5. **No background retry** — the source system is expected to retry the request

---

## 5. Partitioning & Ordering

### Message Key

Every Kafka message uses `subject` (patient UPID) as the message key:

```java
kafkaTemplate.send(inboundTopic, event.getSubject(), event);
```

This guarantees:
- **Per-patient ordering** — all events for the same patient go to the same partition
- **Matcher Service can process events in order** — critical for protocol step matching

### Example Keys

| Patient | Key | Effect |
|---------|-----|--------|
| Marie-Claire KAYITESI | `260225-0002-5501` | All her events → same partition |
| Jean-Baptiste HABIMANA | `260225-0002-5502` | All his events → same partition |

---

## 6. Matcher Service Consumer Contract

The Matcher Service consumes from `cce.events.inbound` with these guarantees from the Collector:

| # | Guarantee | Description |
|---|-----------|-------------|
| 1 | `subject` always present | Used for patient protocol instance lookup |
| 2 | `type` is always present and non-empty | Passed through from emitter; Matcher Service uses `data.resourceType` for Tier 1 structural matching |
| 3 | `data` contains valid payload | FHIR R4 resource or valid JSON object depending on `datacontenttype` |
| 4 | Field names use CloudEvents lowercase convention | e.g., `specversion`, `datacontenttype`, `correlationid` |
| 5 | Kafka key = `subject` | Per-patient ordering |
| 6 | `correlationid` always present | For distributed tracing |
| 7 | Each message maps to an `inbound_event_log` row | Authoritative source of truth |

**What the Collector does NOT populate:**
- `protocolinstanceid` — Matcher Service resolves this from its `protocol_instance` table
- `protocoldefinitionid` — Matcher Service resolves this from its `protocol_definition` table
- `actionid` — Matcher Service determines the matching action/step

---

## 7. Sample Events

### 7.1 Visit Registration (Encounter)

**Kafka Key:** `260225-0002-5501`  
**Kafka Topic:** `cce.events.inbound`

```json
{
  "id": "evt-eb010001-0001-4000-8000-000000000001",
  "source": "rhie-mediator",
  "type": "org.openphc.cce.encounter",
  "specversion": "1.0",
  "subject": "260225-0002-5501",
  "time": "2026-02-25T08:00:00Z",
  "datacontenttype": "application/fhir+json",
  "correlationid": "corr-1343872c-636d-506f-b041-1e571d426932",
  "sourceeventid": "enc-visit-20260225-0001",
  "facilityid": "0002",
  "data": {
    "resourceType": "Encounter",
    "id": "enc-uuid-visit-kicukiro-001",
    "status": "in-progress",
    "class": {
      "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
      "code": "AMB",
      "display": "ambulatory"
    },
    "type": [
      {
        "coding": [
          {
            "system": "http://openphc.org/encounter-types",
            "code": "VISIT_ENCOUNTER",
            "display": "Visit Encounter"
          }
        ]
      }
    ],
    "subject": {
      "reference": "Patient/260225-0002-5501",
      "display": "Marie-Claire KAYITESI"
    },
    "period": {
      "start": "2026-02-25T08:00:00Z"
    }
  }
}
```

### 7.2 Vital Signs (Observation)

**Kafka Key:** `260225-0002-5501`  
**Kafka Topic:** `cce.events.inbound`

```json
{
  "id": "evt-eb010001-0002-4000-8000-000000000002",
  "source": "rhie-mediator",
  "type": "org.openphc.cce.observation",
  "specversion": "1.0",
  "subject": "260225-0002-5501",
  "time": "2026-02-25T08:05:00Z",
  "datacontenttype": "application/fhir+json",
  "correlationid": "corr-1343872c-636d-506f-b041-1e571d426932",
  "sourceeventid": "obs-bp-20260225-0001",
  "facilityid": "0002",
  "data": {
    "resourceType": "Observation",
    "id": "obs-uuid-bp-001",
    "status": "final",
    "category": [
      {
        "coding": [
          {
            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
            "code": "vital-signs",
            "display": "Vital Signs"
          }
        ]
      }
    ],
    "code": {
      "coding": [
        {
          "system": "http://loinc.org",
          "code": "85354-9",
          "display": "Blood Pressure"
        }
      ]
    },
    "subject": {
      "reference": "Patient/260225-0002-5501"
    },
    "effectiveDateTime": "2026-02-25T08:05:00Z",
    "component": [
      {
        "code": {
          "coding": [{ "system": "http://loinc.org", "code": "8480-6", "display": "Systolic" }]
        },
        "valueQuantity": { "value": 120, "unit": "mmHg" }
      },
      {
        "code": {
          "coding": [{ "system": "http://loinc.org", "code": "8462-4", "display": "Diastolic" }]
        },
        "valueQuantity": { "value": 80, "unit": "mmHg" }
      }
    ]
  }
}
```

### 7.3 Diagnosis (Condition)

**Kafka Key:** `260225-0002-5501`  
**Kafka Topic:** `cce.events.inbound`

```json
{
  "id": "evt-eb010001-0006-4000-8000-000000000006",
  "source": "rhie-mediator",
  "type": "org.openphc.cce.condition",
  "specversion": "1.0",
  "subject": "260225-0002-5501",
  "time": "2026-02-25T08:25:00Z",
  "datacontenttype": "application/fhir+json",
  "correlationid": "corr-1343872c-636d-506f-b041-1e571d426932",
  "sourceeventid": "cond-diag-20260225-0001",
  "facilityid": "0002",
  "data": {
    "resourceType": "Condition",
    "id": "cond-uuid-malaria-001",
    "clinicalStatus": {
      "coding": [{ "system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active" }]
    },
    "verificationStatus": {
      "coding": [{ "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status", "code": "confirmed" }]
    },
    "code": {
      "coding": [
        {
          "system": "http://hl7.org/fhir/sid/icd-10",
          "code": "B54",
          "display": "Unspecified malaria"
        }
      ]
    },
    "subject": {
      "reference": "Patient/260225-0002-5501"
    },
    "recordedDate": "2026-02-25T08:25:00Z"
  }
}
```

### 7.4 Medication Prescription (MedicationRequest)

**Kafka Key:** `260225-0002-5501`  
**Kafka Topic:** `cce.events.inbound`

```json
{
  "id": "evt-eb010001-0007-4000-8000-000000000007",
  "source": "rhie-mediator",
  "type": "org.openphc.cce.medicationrequest",
  "specversion": "1.0",
  "subject": "260225-0002-5501",
  "time": "2026-02-25T08:30:00Z",
  "datacontenttype": "application/fhir+json",
  "correlationid": "corr-1343872c-636d-506f-b041-1e571d426932",
  "sourceeventid": "rx-act-20260225-0001",
  "facilityid": "0002",
  "data": {
    "resourceType": "MedicationRequest",
    "id": "rx-uuid-act-001",
    "status": "active",
    "intent": "order",
    "medicationCodeableConcept": {
      "coding": [
        {
          "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
          "code": "825466",
          "display": "Artemether/Lumefantrine 20/120mg"
        }
      ]
    },
    "subject": {
      "reference": "Patient/260225-0002-5501"
    },
    "authoredOn": "2026-02-25T08:30:00Z"
  }
}
```

---

## 8. Metrics

The Collector records these Kafka-related metrics via Micrometer:

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `cce.collector.events.received` | Counter | — | Total events received at POST /v1/events |
| `cce.collector.events.accepted` | Counter | — | Events that passed all validation and were published |
| `cce.collector.events.duplicate` | Counter | — | Duplicate events detected |
| `cce.collector.events.rejected` | Counter | `reason` | Events rejected, tagged by `RejectionReason` enum value |
| `cce.collector.ingestion.duration` | Timer | — | Full ingestion pipeline duration (end-to-end) |
| `cce.collector.kafka.publish.success` | Counter | — | Successful Kafka publishes |
| `cce.collector.kafka.publish.failure` | Counter | — | Failed Kafka publishes |
| Spring Kafka `kafka.producer.*` | — | — | Standard Kafka producer metrics (record-send-rate, record-error-rate, etc.) |

> **Prometheus format:** Micrometer converts dots to underscores and appends `_total` for counters (e.g., `cce_collector_events_received_total`).
