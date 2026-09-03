package org.openphc.cce.collector.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.common.fhir.ClinicalEventTimeExtractor;
import org.openphc.cce.common.fhir.ResourceTypeDetector;

import java.time.OffsetDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link EventDefaultsEnricher}.
 *
 * <p>Verifies the two enrichment rules and confirms that both the entity
 * and the request DTO receive enriched values (so the request can be
 * published to Kafka with defaults applied).
 * No Spring context needed — pure unit tests.</p>
 */
class EventDefaultsEnricherTest {

    private EventDefaultsEnricher enricher;
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        ClinicalEventTimeExtractor extractor =
                new ClinicalEventTimeExtractor(new SimpleMeterRegistry());
        enricher = new EventDefaultsEnricher(extractor, new ResourceTypeDetector());
    }

    /**
     * Builds a valid request with all optional fields absent.
     */
    private EventIngestionRequest requestWithoutOptionals() {
        return EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-001")
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .subject("UPID-12345")
                .datacontenttype("application/fhir+json")
                .data(mapper.valueToTree(Map.of("resourceType", "Encounter")))
                .build();
    }

    /**
     * Builds a valid request with all optional fields pre-set.
     */
    private EventIngestionRequest requestWithOptionals() {
        return EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-002")
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .subject("UPID-12345")
                .data(mapper.valueToTree(Map.of("resourceType", "Encounter")))
                .correlationid("existing-corr-id")
                .time("2026-03-01T10:00:00Z")
                .datacontenttype("application/json")
                .build();
    }

    /**
     * Builds a minimal InboundEvent entity with receivedAt set.
     */
    private InboundEventLog baseEntity() {
        return InboundEventLog.builder()
                .cloudeventsId("evt-001")
                .source("rhie-mediator")
                .rawPayload("{\"resourceType\":\"Encounter\"}")
                .build();
    }

    // ════════════════════════════════════════════════════════════════
    // Correlation ID enrichment
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Correlation ID enrichment")
    class CorrelationIdEnrichment {

        @Test
        @DisplayName("generates correlationId on both entity and request when absent")
        void generatesWhenAbsent() {
            EventIngestionRequest request = requestWithoutOptionals();
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            assertThat(entity.getCorrelationId())
                    .isNotNull()
                    .startsWith("corr-")
                    .hasSize(5 + 36); // "corr-" + UUID (36 chars)
            // Request is also enriched (for Kafka publishing)
            assertThat(request.getCorrelationid())
                    .isEqualTo(entity.getCorrelationId());
        }

        @Test
        @DisplayName("preserves correlationId when present in request")
        void preservesWhenPresent() {
            EventIngestionRequest request = requestWithOptionals();
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            assertThat(entity.getCorrelationId()).isEqualTo("existing-corr-id");
            // Request is NOT mutated
            assertThat(request.getCorrelationid()).isEqualTo("existing-corr-id");
        }

        @Test
        @DisplayName("generates unique correlationId on each call")
        void generatesUniqueIds() {
            EventIngestionRequest req1 = requestWithoutOptionals();
            InboundEventLog ent1 = baseEntity();
            EventIngestionRequest req2 = requestWithoutOptionals();
            InboundEventLog ent2 = baseEntity();

            enricher.enrich(req1, ent1);
            enricher.enrich(req2, ent2);

            assertThat(ent1.getCorrelationId())
                    .isNotEqualTo(ent2.getCorrelationId());
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Time enrichment
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Time enrichment")
    class TimeEnrichment {

        @Test
        @DisplayName("fills time on request when absent")
        void fillsWhenAbsent() {
            EventIngestionRequest request = requestWithoutOptionals();
            InboundEventLog entity = baseEntity();
            OffsetDateTime receivedAt = entity.getReceivedAt();

            enricher.enrich(request, entity);

            // Request is enriched (for Kafka publishing)
            assertThat(request.getTime()).isEqualTo(receivedAt.toString());
        }

        @Test
        @DisplayName("preserves time when present in request")
        void preservesWhenPresent() {
            EventIngestionRequest request = requestWithOptionals();
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            // Request is NOT mutated
            assertThat(request.getTime()).isEqualTo("2026-03-01T10:00:00Z");
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Event time (clinical occurrence time) enrichment
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Event time enrichment")
    class EventTimeEnrichment {

        @Test
        @DisplayName("extracts clinical time from FHIR payload when present")
        void extractsClinicalTime() {
            EventIngestionRequest request = requestWithoutOptionals();
            request.setData(mapper.valueToTree(Map.of(
                    "resourceType", "Encounter",
                    "period", Map.of("start", "2026-03-01T08:00:00Z", "end", "2026-03-01T09:30:00Z"))));
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            // Encounter prefers period.start
            assertThat(entity.getEventTime())
                    .isEqualTo(OffsetDateTime.parse("2026-03-01T08:00:00Z"));
        }

        @Test
        @DisplayName("falls back to envelope time when no clinical field present")
        void fallsBackToEnvelopeTime() {
            EventIngestionRequest request = requestWithOptionals(); // time = 2026-03-01T10:00:00Z, no clinical field
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            assertThat(entity.getEventTime())
                    .isEqualTo(OffsetDateTime.parse("2026-03-01T10:00:00Z"));
        }

        @Test
        @DisplayName("falls back to receivedAt when neither clinical field nor envelope time present")
        void fallsBackToReceivedAt() {
            EventIngestionRequest request = requestWithoutOptionals(); // no time, resourceType has no clinical field
            InboundEventLog entity = baseEntity();
            OffsetDateTime receivedAt = entity.getReceivedAt();

            enricher.enrich(request, entity);

            // enrichTime fills request.time from receivedAt, so eventTime resolves to receivedAt
            assertThat(entity.getEventTime()).isEqualTo(receivedAt);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Type pass-through (NEVER modified)
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Type pass-through")
    class TypePassThrough {

        @Test
        @DisplayName("type is never modified by enrichment")
        void typeNeverModified() {
            EventIngestionRequest request = requestWithoutOptionals();
            String originalType = request.getType();
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            assertThat(request.getType()).isEqualTo(originalType);
        }

        @Test
        @DisplayName("arbitrary type value passes through unchanged")
        void arbitraryTypePassesThrough() {
            EventIngestionRequest request = requestWithoutOptionals();
            request.setType("any.arbitrary.string");
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            assertThat(request.getType()).isEqualTo("any.arbitrary.string");
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Entity sync
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Entity synchronization")
    class EntitySync {

        @Test
        @DisplayName("all enriched values set on both entity and request")
        void allValuesSyncedToEntityAndRequest() {
            EventIngestionRequest request = requestWithoutOptionals();
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            assertThat(entity.getCorrelationId()).isNotNull().startsWith("corr-");

            // Request also receives enriched values (for Kafka publishing)
            assertThat(request.getCorrelationid()).isEqualTo(entity.getCorrelationId());
            assertThat(request.getTime()).isNotNull();
        }

        @Test
        @DisplayName("all preserved values synced to entity")
        void preservedValuesSyncedToEntity() {
            EventIngestionRequest request = requestWithOptionals();
            InboundEventLog entity = baseEntity();

            enricher.enrich(request, entity);

            assertThat(entity.getCorrelationId()).isEqualTo("existing-corr-id");
            assertThat(request.getTime()).isEqualTo("2026-03-01T10:00:00Z");
        }
    }
}
