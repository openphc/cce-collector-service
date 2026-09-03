# CCE Collector Service — Flow Diagrams

Visual diagrams of the Collector Service's processing flows using Mermaid.

---

## 1. Event Ingestion Flow (Main Processing Pipeline)

```mermaid
flowchart TD
    A[HTTP POST /v1/events] --> B{CloudEvents<br/>Envelope Valid?}
    B -->|No| C[400 Bad Request]
    C --> C1[Update inbound_event_log<br/>status: REJECTED<br/>reason: INVALID_ENVELOPE]
    B -->|Yes| D{Duplicate?<br/>source + id in<br/>lookback window}
    D -->|Yes| E[200 OK<br/>status: duplicate]
    D -->|No| F[Persist to<br/>inbound_event_log<br/>status: RECEIVED]
    F --> G2[Apply Defaults<br/>• generate correlationId if absent<br/>• fill time if absent]
    G2 --> H0{datacontenttype?}
    H0 -->|application/fhir+json<br/>or absent| H{FHIR Payload<br/>Valid?}
    H0 -->|application/json| H3{Valid JSON<br/>Object?}
    H0 -->|other| H4[400 Bad Request]
    H4 --> H4a[Update inbound_event_log<br/>status: REJECTED<br/>reason: UNSUPPORTED_CONTENT_TYPE]
    H3 -->|No| H3a[422 Unprocessable]
    H3a --> H3b[Update inbound_event_log<br/>status: REJECTED]
    H3b --> H3c[Update inbound_event_log<br/>status: REJECTED<br/>reason: INVALID_JSON]
    H3 -->|Yes| J[Update inbound_event_log<br/>status: ACCEPTED]
    H -->|No| I[422 Unprocessable]
    I --> I1[Update inbound_event_log<br/>status: REJECTED]
    I1 --> I2[Update inbound_event_log<br/>status: REJECTED<br/>reason: INVALID_FHIR]
    H -->|Yes| J[Update inbound_event_log<br/>status: ACCEPTED]
    J --> K{Kafka Publish<br/>Successful?}
    K -->|Yes| L[202 Accepted<br/>Ingestion Receipt]
    K -->|No| M[Update inbound_event_log<br/>status: REJECTED<br/>reason: KAFKA_PUBLISH_FAILURE]
    M --> N[500 Internal Server Error]

    style A fill:#4a90d9,color:#fff
    style E fill:#f0ad4e,color:#000
    style L fill:#5cb85c,color:#fff
    style C fill:#d9534f,color:#fff
    style I fill:#d9534f,color:#fff
    style C1 fill:#d9534f,color:#fff
    style I2 fill:#d9534f,color:#fff
    style N fill:#d9534f,color:#fff
    style H4 fill:#d9534f,color:#fff
    style H4a fill:#d9534f,color:#fff
    style H3a fill:#d9534f,color:#fff
    style H3c fill:#d9534f,color:#fff
```

---

## 2. Sequence Diagram — Successful Event Ingestion

```mermaid
sequenceDiagram
    participant Client as External System<br/>(openHIM / EMR)
    participant Controller as EventIngestionController
    participant Service as EventIngestionService
    participant Validator as CloudEventValidator
    participant Dedup as DeduplicationService
    participant Repo as InboundEventLogRepository
    participant Defaults as EventDefaultsEnricher
    participant PV as PayloadValidator
    participant Kafka as Kafka Broker

    Client->>Controller: POST /v1/events (CloudEvents JSON)
    Controller->>Service: ingest(request)
    Service->>Validator: validate(request)
    Validator-->>Service: ✓ valid

    Service->>Dedup: isDuplicate(source, id)
    Dedup-->>Service: false

    Service->>Repo: save(inboundEvent, status=RECEIVED)
    Repo-->>Service: inboundEvent

    Service->>Defaults: ensureCorrelationId(correlationid)
    Defaults-->>Service: corr-<uuid>
    Service->>Defaults: ensureEventTime(time)
    Defaults-->>Service: OffsetDateTime

    Service->>PV: validate(request)
    PV-->>Service: ✓ valid FHIR R4

    Service->>Repo: save(inboundEvent, status=ACCEPTED)

    Service->>Kafka: send(topic, key=subject, value=CloudEvents JSON)
    Kafka-->>Service: RecordMetadata (topic, partition, offset)

    Service-->>Controller: EventIngestionResponse
    Controller-->>Client: 202 Accepted {eventId, status: accepted, correlationId}
```

---

## 3. Sequence Diagram — Validation Failure (FHIR)

```mermaid
sequenceDiagram
    participant Client as External System
    participant Controller as EventIngestionController
    participant Service as EventIngestionService
    participant Validator as CloudEventValidator
    participant Dedup as DeduplicationService
    participant Repo as InboundEventLogRepository
    participant Defaults as EventDefaultsEnricher
    participant PV as PayloadValidator
    participant RS as RejectionService

    Client->>Controller: POST /v1/events (invalid FHIR data)
    Controller->>Service: ingest(request)
    Service->>Validator: validate(request)
    Validator-->>Service: ✓ envelope valid

    Service->>Dedup: isDuplicate(source, id)
    Dedup-->>Service: false

    Service->>Repo: save(inboundEvent, status=RECEIVED)
    Service->>Defaults: apply server-side defaults
    Defaults-->>Service: enriched values

    Service->>PV: validate(request)
    PV-->>Service: ✗ PayloadValidationException

    Service->>RS: recordRejection(inboundEvent, INVALID_FHIR, errorDetails)

    Service-->>Controller: throw PayloadValidationException
    Controller-->>Client: 422 Unprocessable Entity {error details}
```

---

## 4. Sequence Diagram — Duplicate Detection

```mermaid
sequenceDiagram
    participant Client as External System
    participant Controller as EventIngestionController
    participant Service as EventIngestionService
    participant Validator as CloudEventValidator
    participant Dedup as DeduplicationService

    Client->>Controller: POST /v1/events (same id + source as before)
    Controller->>Service: ingest(request)
    Service->>Validator: validate(request)
    Validator-->>Service: ✓ valid

    Service->>Dedup: isDuplicate(source, id)
    Note over Dedup: Query inbound_event_log WHERE<br/>cloudevents_id = ? AND source = ?<br/>AND received_at > now() - lookback
    Dedup-->>Service: true (DuplicateEventException)

    Service-->>Controller: throw DuplicateEventException
    Controller-->>Client: 200 OK {eventId, status: duplicate}
    Note over Client: Idempotent — no side effects
```

---

## 5. System Context — Data Flow

```mermaid
flowchart LR
    subgraph External["External Systems"]
        EMR1[eBUZIMA EMR]
        EMR2[SmartCare]
        CHW[CHW App]
        LAB[Lab Systems]
    end

    subgraph RHIE["RHIE Layer"]
        OH[openHIM Mediator]
    end

    subgraph CCE["CCE Platform"]
        GW[CCE Gateway<br/>auth + routing]
        CS[Collector Service<br/>validate → dedup<br/>→ publish]
        PG[(PostgreSQL<br/>inbound_event_log)]
        KF[Kafka<br/>cce.events.inbound]
        COMP[Matcher Service]
    end

    EMR1 --> OH
    EMR2 --> OH
    CHW --> OH
    LAB --> OH
    OH -->|HTTP POST<br/>CloudEvents| GW
    GW -->|authenticated| CS
    CS -->|persist| PG
    CS -->|publish| KF
    KF -->|consume| COMP

    style CS fill:#4a90d9,color:#fff
    style KF fill:#5cb85c,color:#fff
    style PG fill:#f0ad4e,color:#000
```

---

## 6. Database Entity

```mermaid
erDiagram
    inbound_event_log {
        UUID id PK
        VARCHAR cloudevents_id
        VARCHAR source
        VARCHAR correlation_id
        JSONB raw_payload
        VARCHAR status
        VARCHAR rejection_reason
        TEXT error_details
        TIMESTAMPTZ event_time
        TIMESTAMPTZ received_at
    }
```

---

## 7. Field Transformation Pipeline

```mermaid
flowchart LR
    subgraph Input["HTTP Request (lowercase)"]
        A1["specversion: '1.0'"]
        A2["type: 'org.openphc.cce.encounter'"]
        A3["correlationid: null"]
        A4["time: null"]
        A5["facilityid: '0002'"]
    end

    subgraph Defaults["EventDefaultsEnricher"]
        N2["correlationid → corr-<uuid>"]
        N3["time → server received_at"]
        N4["event_time → clinical time / envelope time / received_at<br/>(persisted to entity only, not published)"]
    end

    subgraph Output["Kafka Message (lowercase)"]
        B1["specversion: '1.0'"]
        B2["type: 'org.openphc.cce.encounter'"]
        B3["correlationid: 'corr-abc-123'"]
        B4["time: '2026-02-25T08:00:00Z'"]
        B5["facilityid: '0002'"]
    end

    A1 --> B1
    A2 --> V1 --> B2
    A3 --> N2 --> B3
    A4 --> N3 --> B4
    A5 --> B5

    style Validate fill:#5cb85c,color:#fff
    style Defaults fill:#4a90d9,color:#fff
```
