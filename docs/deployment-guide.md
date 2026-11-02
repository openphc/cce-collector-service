# CCE Collector Service — Deployment Guide

## 1. Prerequisites

| Dependency | Minimum Version | Notes |
|-----------|----------------|-------|
| Java | 21 (LTS) | Eclipse Temurin recommended |
| Gradle | 8.x | Only for building from source (wrapper included) |
| PostgreSQL | 16+ | Primary datastore |
| Apache Kafka | 3.7+ | KRaft mode (no ZooKeeper) |
| Docker | 24+ | For containerized deployment |
| Docker Compose | 2.20+ | For local development |

---

## 2. Local Development Setup

### 2.1 Start Infrastructure

PostgreSQL and Kafka are shared infrastructure managed outside this service. Start them from the shared `deploy-scripts` repository:

```bash
cd /path/to/deploy-scripts
docker compose up -d
```

This creates the `cce-net` Docker bridge network and starts:
- **PostgreSQL** on port `5433` (user: `cce_user`, password: `cce_pass`, database: `ccedb`)
- **Kafka** on port `29092` (host) / `9092` (inter-container, KRaft mode, single broker)
- **Kafka UI** on port `8090`

Then deploy the collector service:

```bash
cd /path/to/cce-collector-service
docker compose up -d
```

The collector service connects to the shared infra via the external `deploy-scripts_cce-net` network.

### 2.2 Build the Application

```bash
./gradlew build -x test
```

### 2.3 Run with Local Profile

```bash
java -jar build/libs/cce-collector-service-1.0.0-SNAPSHOT.jar \
  --spring.profiles.active=local
```

Or using Gradle:

```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

The application will start on port **8081** (local profile override).

### 2.4 Verify

```bash
# Health check
curl http://localhost:8081/actuator/health

# Submit a test event
curl -X POST http://localhost:8081/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "specversion": "1.0",
    "id": "test-event-001",
    "source": "test/local",
    "type": "cce.encounter.created",
    "subject": "patient/TEST-001",
    "data": { "resourceType": "Encounter", "status": "finished" }
  }'
```

---

## 3. Configuration Profiles

| Profile | File | Description |
|---------|------|-------------|
| (default) | `application.yml` | Base configuration |
| `local` | `application-local.yml` | Debug logging, relaxed pool sizes |
| `staging` | `application-staging.yml` | Moderate pool sizes |
| `production` | `application-production.yml` | Optimized pool sizes, shorter retry intervals |

Activate a profile:
```bash
java -jar build/libs/cce-collector-service-1.0.0-SNAPSHOT.jar \
  --spring.profiles.active=production
```

---

## 4. Environment Variables

All configuration can be overridden via environment variables using Spring Boot's relaxed binding:

### Server

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8080` | HTTP listen port |
| `TOMCAT_THREADS_MAX` | `200` | Maximum Tomcat worker threads |

### Database

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5433/ccedb` | JDBC URL |
| `SPRING_DATASOURCE_USERNAME` | `cce_user` | Database username |
| `SPRING_DATASOURCE_PASSWORD` | `cce_pass` | Database password |
| `SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE` | `10` | Connection pool max size |
| `SPRING_DATASOURCE_HIKARI_MINIMUM_IDLE` | `5` | Connection pool min idle |
| `SPRING_DATASOURCE_HIKARI_IDLE_TIMEOUT` | `300000` | Idle connection timeout (ms) |
| `SPRING_DATASOURCE_HIKARI_MAX_LIFETIME` | `600000` | Max connection lifetime (ms) |
| `SPRING_DATASOURCE_HIKARI_CONNECTION_TIMEOUT` | `30000` | Connection acquisition timeout (ms) |

### Kafka

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker(s) |
| `SPRING_KAFKA_PRODUCER_ACKS` | `all` | Producer acknowledgement level |
| `SPRING_KAFKA_PRODUCER_RETRIES` | `3` | Producer retry count |
| `KAFKA_LINGER_MS` | `5` | Producer batching delay (ms) |
| `KAFKA_BATCH_SIZE` | `16384` | Producer batch size (bytes) |
| `KAFKA_BUFFER_MEMORY` | `33554432` | Producer buffer memory (bytes) |

### Application-Specific

| Variable | Default | Description |
|----------|---------|-------------|
| `CCE_COLLECTOR_FHIR_STRICT_VALIDATION` | `false` | Enable strict FHIR validation |
| `CCE_COLLECTOR_FHIR_PATIENT_IDENTIFIER_SYSTEM` | `http://openphc.org/identifier/upid` | System URI for extracting UPID from Patient.identifier[] |
| `CCE_COLLECTOR_DEDUP_LOOKBACK_DAYS` | `30` | Dedup lookback window (days) |
| `CCE_COLLECTOR_MAX_PAYLOAD_SIZE` | `1048576` | Max event payload size (bytes, ~1 MB) |
| `CCE_KAFKA_TOPICS_INBOUND_EVENTS` | `cce.events.inbound` | Inbound events topic. Not `CCE_COLLECTOR_`-prefixed: the name is bound by cce-common-util's `KafkaTopicProperties`, the same property the Matcher Service reads to find the topic it consumes from |
| `CCE_COLLECTOR_KAFKA_PUBLISH_TIMEOUT_SECONDS` | `30` | Synchronous Kafka publish timeout (seconds) |
| `CCE_COLLECTOR_KAFKA_TOPIC_PARTITIONS` | `25` | Inbound topic partition count |
| `CCE_COLLECTOR_KAFKA_TOPIC_REPLICATION_FACTOR` | `1` | Inbound topic replication factor |
| `CCE_COLLECTOR_KAFKA_TOPIC_RETENTION_MS` | `604800000` | Inbound topic retention (ms, default 7 days) |
| `CCE_COLLECTOR_KAFKA_TOPIC_CLEANUP_POLICY` | `delete` | Inbound topic cleanup policy |
| `CCE_COLLECTOR_KAFKA_TOPIC_MIN_INSYNC_REPLICAS` | `1` | Inbound topic min in-sync replicas |

---

## 5. Docker Build

### Build Image

The build context is the directory holding both `cce-collector-service` and `cce-common-util`
(`settings.gradle` includes `../cce-common-util` as a composite build). Run from that directory:

```bash
docker build -f cce-collector-service/Dockerfile -t cce-collector-service:latest .
```

The Dockerfile uses a **multi-stage build**:
1. Stage 1 (`builder`): `eclipse-temurin:21-jdk-alpine` — compiles `./gradlew build` against `cce-common-util`
2. Stage 2 (`runtime`): `eclipse-temurin:21-jre-alpine` — minimal runtime image, non-root `cce` user, `HEALTHCHECK` on `/actuator/health`

### Run Container

```bash
docker run -d \
  --name cce-collector \
  -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=production \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://db-host:5432/ccedb \
  -e SPRING_DATASOURCE_USERNAME=cce_user \
  -e SPRING_DATASOURCE_PASSWORD=cce_pass \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka-host:9092 \
  cce-collector-service:latest
```

---

## 6. Database Migrations

Flyway runs automatically on startup. Migrations are located in `src/main/resources/db/migration/`:

| Migration | Description |
|-----------|-------------|
| `V1__create_inbound_event_log.sql` | `inbound_event_log` table with dedup constraint and rejection tracking columns |

### Manual Migration Execution

```bash
./gradlew flywayMigrate \
  -Pflyway.url=jdbc:postgresql://localhost:5432/ccedb \
  -Pflyway.user=cce_user \
  -Pflyway.password=cce_pass
```

---

## 7. Kubernetes Deployment

### Health Probes

Configure Kubernetes liveness and readiness probes:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cce-collector-service
  labels:
    app: cce-collector
spec:
  replicas: 2
  selector:
    matchLabels:
      app: cce-collector
  template:
    metadata:
      labels:
        app: cce-collector
    spec:
      containers:
        - name: cce-collector
          image: cce-collector-service:latest
          ports:
            - containerPort: 8080
          env:
            - name: SPRING_PROFILES_ACTIVE
              value: production
            - name: SPRING_DATASOURCE_URL
              valueFrom:
                secretKeyRef:
                  name: cce-db-secret
                  key: url
            - name: SPRING_DATASOURCE_USERNAME
              valueFrom:
                secretKeyRef:
                  name: cce-db-secret
                  key: username
            - name: SPRING_DATASOURCE_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: cce-db-secret
                  key: password
            - name: SPRING_KAFKA_BOOTSTRAP_SERVERS
              valueFrom:
                configMapKeyRef:
                  name: cce-kafka-config
                  key: bootstrap-servers
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8080
            initialDelaySeconds: 15
            periodSeconds: 5
            failureThreshold: 3
          resources:
            requests:
              cpu: 500m
              memory: 512Mi
            limits:
              cpu: "1"
              memory: 1Gi
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: cce-collector-service
spec:
  selector:
    app: cce-collector
  ports:
    - port: 80
      targetPort: 8080
  type: ClusterIP
```

### Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cce-db-secret
type: Opaque
stringData:
  url: jdbc:postgresql://pg-host:5432/ccedb
  username: cce_user
  password: <your-password>
```

---

## 8. Kafka Topic Setup

The application auto-creates the `cce.events.inbound` topic on startup via a `NewTopic` bean in `KafkaTopicConfig`. Topic properties are configurable via `cce.kafka.topic-config.*` in `application.yml` or environment variables.

### Default Topic Configuration

| Setting | Default | Production Override | Env Var |
|---------|---------|--------------------|---------|
| `partitions` | `25` | `25` | `CCE_COLLECTOR_KAFKA_TOPIC_PARTITIONS` |
| `replication-factor` | `1` | `3` | `CCE_COLLECTOR_KAFKA_TOPIC_REPLICATION_FACTOR` |
| `retention.ms` | `604800000` (7 days) | `604800000` | `CCE_COLLECTOR_KAFKA_TOPIC_RETENTION_MS` |
| `cleanup.policy` | `delete` | `delete` | `CCE_COLLECTOR_KAFKA_TOPIC_CLEANUP_POLICY` |
| `min.insync.replicas` | `1` | `2` | `CCE_COLLECTOR_KAFKA_TOPIC_MIN_INSYNC_REPLICAS` |

> **Note:** If the topic already exists, the broker will not alter partitions or replication factor. To change partitions on an existing topic, use:
> ```bash
> kafka-topics.sh --alter \
>   --topic cce.events.inbound \
>   --partitions 25 \
>   --bootstrap-server kafka:9092
> ```

To pre-create the topic manually (e.g., if broker auto-creation is disabled):

```bash
# Inbound events topic (25 partitions, replication factor 3 for production)
kafka-topics.sh --create \
  --topic cce.events.inbound \
  --partitions 25 \
  --replication-factor 3 \
  --bootstrap-server kafka:9092
```

> **Note:** Rejected events are tracked in `inbound_event_log` (database). No dead-letter Kafka topic is implemented.

### Recommended Production Topic Configuration

| Setting | Value | Rationale |
|---------|-------|-----------|
| `retention.ms` | `604800000` (7 days) | Sufficient replay window |
| `min.insync.replicas` | `2` | HA with `acks=all` |
| `cleanup.policy` | `delete` | Event stream, not compacted |

---

## 9. Production Checklist

- [ ] PostgreSQL: Create database `ccedb`, grant permissions to `cce_user`
- [ ] PostgreSQL: Tune `shared_buffers`, `work_mem`, `effective_cache_size`
- [ ] Kafka: Verify topic auto-creation on startup (25 partitions, replication factor 3 via production profile)
- [ ] Kafka: Confirm `min.insync.replicas=2` on inbound topic
- [ ] Application: Set `SPRING_PROFILES_ACTIVE=production`
- [ ] Application: Set real database credentials via secrets
- [ ] Application: Verify graceful shutdown (`server.shutdown=graceful`, `timeout-per-shutdown-phase=30s`)
- [ ] Monitoring: Expose `/actuator/prometheus` to Prometheus scraper
- [ ] Logging: Configure log aggregation (ELK/Loki) — JSON structured output enabled by default
- [ ] TLS: Terminate TLS at load balancer or ingress controller
- [ ] Backups: Schedule PostgreSQL pg_dump for `inbound_event_log`
