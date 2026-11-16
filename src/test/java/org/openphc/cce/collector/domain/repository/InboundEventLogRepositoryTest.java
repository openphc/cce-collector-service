package org.openphc.cce.collector.domain.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link InboundEventLogRepository}.
 *
 * <p>Uses H2 in-memory database with test profile (ddl-auto: create-drop).
 * Validates all custom query methods, JSONB field persistence, and enum mapping.</p>
 */
@DataJpaTest
@ActiveProfiles("test")
class InboundEventLogRepositoryTest {

    @Autowired
    private InboundEventLogRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    // ─── Helper ────────────────────────────────────────────────────────

    private InboundEventLog buildEvent(String cloudeventsId, String source, InboundStatus status) {
        return InboundEventLog.builder()
                .cloudeventsId(cloudeventsId)
                .source(source)
                .rawPayload("{\"resourceType\":\"Bundle\",\"entry\":[]}")
                .status(status)
                .build();
    }

    // ─── findByCloudeventsIdAndSource ──────────────────────────────────

    @Nested
    @DisplayName("findByCloudeventsIdAndSource")
    class FindByCloudeventsIdAndSource {

        @Test
        @DisplayName("returns event when exact id + source match exists")
        void returnsMatchingEvent() {
            InboundEventLog saved = repository.save(
                    buildEvent("evt-001", "urn:source:a", InboundStatus.RECEIVED));

            Optional<InboundEventLog> result =
                    repository.findByCloudeventsIdAndSource("evt-001", "urn:source:a");

            assertThat(result).isPresent();
            assertThat(result.get().getId()).isEqualTo(saved.getId());
        }

        @Test
        @DisplayName("returns empty when source does not match")
        void emptyWhenSourceMismatch() {
            repository.save(buildEvent("evt-001", "urn:source:a", InboundStatus.RECEIVED));

            Optional<InboundEventLog> result =
                    repository.findByCloudeventsIdAndSource("evt-001", "urn:source:b");

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("returns empty when id does not match")
        void emptyWhenIdMismatch() {
            repository.save(buildEvent("evt-001", "urn:source:a", InboundStatus.RECEIVED));

            Optional<InboundEventLog> result =
                    repository.findByCloudeventsIdAndSource("evt-999", "urn:source:a");

            assertThat(result).isEmpty();
        }
    }

    // ─── existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter ─────

    @Nested
    @DisplayName("existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter")
    class ExistsByStatusAndLookbackWindow {

        @Test
        @DisplayName("returns true when an accepted event exists after the lookback timestamp")
        void trueWhenAcceptedWithinWindow() {
            InboundEventLog event = buildEvent("evt-002", "urn:source:a", InboundStatus.ACCEPTED);
            event.setReceivedAt(OffsetDateTime.now(ZoneOffset.UTC));
            repository.save(event);

            boolean exists = repository.existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter(
                    "evt-002", "urn:source:a", InboundStatus.ACCEPTED,
                    OffsetDateTime.now(ZoneOffset.UTC).minusHours(1));

            assertThat(exists).isTrue();
        }

        @Test
        @DisplayName("returns false when the accepted event is before the lookback timestamp")
        void falseWhenOutsideWindow() {
            InboundEventLog event = buildEvent("evt-002", "urn:source:a", InboundStatus.ACCEPTED);
            event.setReceivedAt(OffsetDateTime.now(ZoneOffset.UTC).minusDays(2));
            repository.save(event);

            boolean exists = repository.existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter(
                    "evt-002", "urn:source:a", InboundStatus.ACCEPTED,
                    OffsetDateTime.now(ZoneOffset.UTC).minusHours(1));

            assertThat(exists).isFalse();
        }

        @Test
        @DisplayName("returns false when the earlier attempt was rejected, so the retry is not a duplicate")
        void falseWhenEarlierAttemptWasRejected() {
            InboundEventLog event = buildEvent("evt-002", "urn:source:a", InboundStatus.REJECTED);
            event.setReceivedAt(OffsetDateTime.now(ZoneOffset.UTC));
            repository.save(event);

            boolean exists = repository.existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter(
                    "evt-002", "urn:source:a", InboundStatus.ACCEPTED,
                    OffsetDateTime.now(ZoneOffset.UTC).minusHours(1));

            assertThat(exists).isFalse();
        }

        @Test
        @DisplayName("returns false when the earlier attempt is still RECEIVED")
        void falseWhenEarlierAttemptIsStillReceived() {
            InboundEventLog event = buildEvent("evt-002", "urn:source:a", InboundStatus.RECEIVED);
            event.setReceivedAt(OffsetDateTime.now(ZoneOffset.UTC));
            repository.save(event);

            boolean exists = repository.existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter(
                    "evt-002", "urn:source:a", InboundStatus.ACCEPTED,
                    OffsetDateTime.now(ZoneOffset.UTC).minusHours(1));

            assertThat(exists).isFalse();
        }
    }

    // ─── findByStatusAndRejectionReasonOrderByReceivedAtAsc ───────────

    @Nested
    @DisplayName("findByStatusAndRejectionReasonOrderByReceivedAtAsc")
    class FindByStatusAndRejectionReason {

        @Test
        @DisplayName("returns only the events with that status and reason, oldest first, up to the page size")
        void returnsOldestMatchingEventsFirst() {
            saveRejected("evt-new", "KAFKA_PUBLISH_FAILURE", 1);
            saveRejected("evt-old", "KAFKA_PUBLISH_FAILURE", 3);
            saveRejected("evt-middle", "KAFKA_PUBLISH_FAILURE", 2);
            saveRejected("evt-invalid", "INVALID_FHIR", 4);
            repository.save(buildEvent("evt-accepted", "urn:source:a", InboundStatus.ACCEPTED));

            List<InboundEventLog> result = repository.findByStatusAndRejectionReasonOrderByReceivedAtAsc(
                    InboundStatus.REJECTED, "KAFKA_PUBLISH_FAILURE", PageRequest.of(0, 2));

            assertThat(result).extracting(InboundEventLog::getCloudeventsId)
                    .containsExactly("evt-old", "evt-middle");
        }

        private void saveRejected(String cloudeventsId, String rejectionReason, int hoursAgo) {
            InboundEventLog event = buildEvent(cloudeventsId, "urn:source:a", InboundStatus.REJECTED);
            event.setRejectionReason(rejectionReason);
            event.setReceivedAt(OffsetDateTime.now(ZoneOffset.UTC).minusHours(hoursAgo));
            repository.save(event);
        }
    }

    // ─── findByStatusOrderByReceivedAtDesc ────────────────────────────

    @Nested
    @DisplayName("findByStatusOrderByReceivedAtDesc")
    class FindByStatus {

        @Test
        @DisplayName("returns only events with matching status, ordered by receivedAt desc")
        void filtersAndOrders() {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

            // rejected (should be returned)
            InboundEventLog r1 = buildEvent("rej-1", "urn:src:a", InboundStatus.REJECTED);
            r1.setReceivedAt(now.minusMinutes(10));
            r1.setRejectionReason(RejectionReason.INVALID_FHIR.name());
            repository.save(r1);

            InboundEventLog r2 = buildEvent("rej-2", "urn:src:a", InboundStatus.REJECTED);
            r2.setReceivedAt(now.minusMinutes(5));
            r2.setRejectionReason(RejectionReason.INVALID_JSON.name());
            repository.save(r2);

            // accepted (should NOT be returned)
            repository.save(buildEvent("acc-1", "urn:src:a", InboundStatus.ACCEPTED));

            Page<InboundEventLog> page = repository
                    .findByStatusOrderByReceivedAtDesc(
                            InboundStatus.REJECTED, PageRequest.of(0, 10));

            assertThat(page.getTotalElements()).isEqualTo(2);
            assertThat(page.getContent().get(0).getCloudeventsId()).isEqualTo("rej-2"); // more recent first
            assertThat(page.getContent().get(1).getCloudeventsId()).isEqualTo("rej-1");
        }

        @Test
        @DisplayName("returns empty page when no matching events")
        void emptyWhenNone() {
            repository.save(buildEvent("acc-1", "urn:src:a", InboundStatus.ACCEPTED));

            Page<InboundEventLog> page = repository
                    .findByStatusOrderByReceivedAtDesc(
                            InboundStatus.REJECTED, PageRequest.of(0, 10));

            assertThat(page.getTotalElements()).isZero();
        }
    }

    // ─── countByStatus ────────────────────────────────────────────────

    @Nested
    @DisplayName("countByStatus")
    class CountByStatus {

        @Test
        @DisplayName("counts only matching status")
        void countsCorrectly() {
            // 2 rejected
            repository.save(buildEvent("rej-1", "urn:src:a", InboundStatus.REJECTED));
            repository.save(buildEvent("rej-2", "urn:src:b", InboundStatus.REJECTED));

            // 1 accepted
            repository.save(buildEvent("acc-1", "urn:src:a", InboundStatus.ACCEPTED));

            assertThat(repository.countByStatus(InboundStatus.REJECTED)).isEqualTo(2);
            assertThat(repository.countByStatus(InboundStatus.ACCEPTED)).isEqualTo(1);
        }
    }

    // ─── JSONB persistence ────────────────────────────────────────────

    @Nested
    @DisplayName("JSONB raw_payload persistence")
    class JsonbPersistence {

        @Test
        @DisplayName("stores and retrieves JSON string in rawPayload column")
        void serializesAndDeserializes() {
            String jsonPayload = "{\"resourceType\":\"Observation\",\"status\":\"final\","
                    + "\"code\":{\"coding\":[{\"system\":\"http://loinc.org\",\"code\":\"85354-9\"}]}}";

            InboundEventLog event = buildEvent("json-1", "urn:src:a", InboundStatus.RECEIVED);
            event.setRawPayload(jsonPayload);
            repository.save(event);

            InboundEventLog loaded = repository.findById(event.getId()).orElseThrow();
            assertThat(loaded.getRawPayload()).isEqualTo(jsonPayload);
        }

        @Test
        @DisplayName("handles nested JSON with arrays and special characters")
        void handlesComplexJson() {
            String complexJson = "{\"entry\":[{\"resource\":{\"text\":\"value with \\\"quotes\\\" and ünïcödë\"}}]}";

            InboundEventLog event = buildEvent("json-2", "urn:src:a", InboundStatus.RECEIVED);
            event.setRawPayload(complexJson);
            repository.save(event);

            InboundEventLog loaded = repository.findById(event.getId()).orElseThrow();
            assertThat(loaded.getRawPayload()).isEqualTo(complexJson);
        }
    }

    // ─── Enum mapping ─────────────────────────────────────────────────

    @Nested
    @DisplayName("Enum mapping")
    class EnumMapping {

        @Test
        @DisplayName("status enum persists as STRING, not ordinal")
        void statusPersistedAsString() {
            InboundEventLog event = buildEvent("enum-1", "urn:src:a", InboundStatus.DUPLICATE);
            repository.save(event);

            InboundEventLog loaded = repository.findById(event.getId()).orElseThrow();
            assertThat(loaded.getStatus()).isEqualTo(InboundStatus.DUPLICATE);
        }

        @Test
        @DisplayName("all InboundStatus values can be persisted and loaded")
        void allStatusValuesRoundTrip() {
            for (InboundStatus status : InboundStatus.values()) {
                InboundEventLog event = buildEvent("status-" + status.name(), "urn:src:a", status);
                repository.save(event);

                InboundEventLog loaded = repository.findById(event.getId()).orElseThrow();
                assertThat(loaded.getStatus()).isEqualTo(status);
            }
        }

        @Test
        @DisplayName("rejection_reason stored as VARCHAR string")
        void rejectionReasonStoredAsString() {
            InboundEventLog event = buildEvent("reason-1", "urn:src:a", InboundStatus.REJECTED);
            event.setRejectionReason(RejectionReason.KAFKA_PUBLISH_FAILURE.name());
            repository.save(event);

            InboundEventLog loaded = repository.findById(event.getId()).orElseThrow();
            assertThat(loaded.getRejectionReason()).isEqualTo("KAFKA_PUBLISH_FAILURE");
        }
    }

    // ─── Builder defaults ─────────────────────────────────────────────

    @Nested
    @DisplayName("Builder defaults")
    class BuilderDefaults {

        @Test
        @DisplayName("default values are set correctly by builder")
        void defaultsApplied() {
            InboundEventLog event = InboundEventLog.builder()
                    .cloudeventsId("def-1")
                    .source("urn:src:a")
                    .rawPayload("{}")
                    .build();

            assertThat(event.getStatus()).isEqualTo(InboundStatus.RECEIVED);
            assertThat(event.getReceivedAt()).isNotNull();
        }
    }

    // ─── UUIDv7 generation ────────────────────────────────────────────

    @Nested
    @DisplayName("UUIDv7 generation")
    class UuidV7Generation {

        @Test
        @DisplayName("id is generated automatically on persist via @PrePersist")
        void idGeneratedOnPersist() {
            InboundEventLog event = buildEvent("uuid-1", "urn:src:a", InboundStatus.RECEIVED);
            assertThat(event.getId()).isNull();

            InboundEventLog saved = repository.save(event);
            assertThat(saved.getId()).isNotNull();
        }

        @Test
        @DisplayName("generated ids are time-ordered (monotonically increasing)")
        void idsAreTimeOrdered() throws InterruptedException {
            InboundEventLog e1 = repository.save(buildEvent("uuid-o1", "urn:src:a", InboundStatus.RECEIVED));
            Thread.sleep(2); // ensure different millisecond for UUIDv7 timestamp
            InboundEventLog e2 = repository.save(buildEvent("uuid-o2", "urn:src:b", InboundStatus.RECEIVED));

            assertThat(e1.getId()).isLessThan(e2.getId());
        }
    }
}
