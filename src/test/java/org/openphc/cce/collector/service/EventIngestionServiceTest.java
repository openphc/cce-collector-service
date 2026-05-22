package org.openphc.cce.collector.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.api.dto.EventIngestionResponse;
import org.openphc.cce.collector.api.exception.CloudEventValidationException;
import org.openphc.cce.collector.api.exception.DuplicateEventException;
import org.openphc.cce.collector.api.exception.PayloadValidationException;
import org.openphc.cce.collector.api.exception.KafkaPublishException;
import org.openphc.cce.collector.config.KafkaTopicProperties;
import org.openphc.cce.collector.domain.model.InboundEvent;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventRepository;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link EventIngestionService}.
 *
 * <p>Verifies the 8-step ingestion pipeline with mocked dependencies.
 * Covers happy paths (FHIR and JSON), all failure modes (envelope,
 * payload, duplicate, Kafka), payload size enforcement, and step
 * ordering.</p>
 */
@ExtendWith(MockitoExtension.class)
class EventIngestionServiceTest {

    @Mock private CloudEventValidator cloudEventValidator;
    @Mock private DeduplicationService deduplicationService;
    @Mock private InboundEventRepository repository;
    @Mock private EventDefaultsEnricher enricher;
    @Mock private PayloadValidator payloadValidator;
    @Mock private EventPublisher eventPublisher;
    @Mock private RejectionService rejectionService;
    @Mock private IngestionSummaryService ingestionSummaryService;

    private SimpleMeterRegistry meterRegistry;
    private EventIngestionService service;
    private KafkaTopicProperties kafkaTopicProperties;

    private static final long MAX_PAYLOAD_SIZE = 1_048_576L; // 1 MB
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final String CLOUD_EVENTS_ID = "evt-001";
    private static final String SOURCE = "rhie-mediator";
    private static final String TYPE = "org.openphc.encounter.created";
    private static final String SUBJECT = "patient-upid-123";
    private static final String CORRELATION_ID = "corr-abc";

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        kafkaTopicProperties = new KafkaTopicProperties();
        service = new EventIngestionService(
                cloudEventValidator,
                deduplicationService,
                repository,
                enricher,
                payloadValidator,
                eventPublisher,
                rejectionService,
                ingestionSummaryService,
                kafkaTopicProperties,
                meterRegistry,
                MAX_PAYLOAD_SIZE
        );

        // Default: validatePayload returns a passing result (lenient so tests
        // that never reach Step 6 don't trigger unnecessary-stubbing errors)
        lenient().when(payloadValidator.validatePayload(any()))
                .thenReturn(PayloadValidationResult.builder()
                        .valid(true)
                        .errors(List.of())
                        .build());
    }

    // ════════════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════════════

    private EventIngestionRequest buildValidFhirRequest() {
        return EventIngestionRequest.builder()
                .specversion("1.0")
                .id(CLOUD_EVENTS_ID)
                .source(SOURCE)
                .type(TYPE)
                .subject(SUBJECT)
                .datacontenttype("application/fhir+json")
                .data(fhirEncounterNode())
                .correlationid(CORRELATION_ID)
                .build();
    }

    private EventIngestionRequest buildValidJsonRequest() {
        return EventIngestionRequest.builder()
                .specversion("1.0")
                .id(CLOUD_EVENTS_ID)
                .source(SOURCE)
                .type(TYPE)
                .subject(SUBJECT)
                .datacontenttype("application/json")
                .data(plainJsonNode())
                .build();
    }

    private JsonNode fhirEncounterNode() {
        try {
            return MAPPER.readTree("""
                    {
                      "resourceType": "Encounter",
                      "id": "enc-1",
                      "status": "finished",
                      "class": {"code": "AMB"},
                      "subject": {"reference": "Patient/patient-upid-123"}
                    }
                    """);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode plainJsonNode() {
        try {
            return MAPPER.readTree("{\"key\": \"value\"}");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Configure repository.save() to simulate JPA @PrePersist (set UUID)
     * and return the entity.
     */
    private void stubRepositorySave() {
        when(repository.save(any(InboundEvent.class))).thenAnswer(invocation -> {
            InboundEvent event = invocation.getArgument(0);
            if (event.getId() == null) {
                event.setId(EVENT_ID);
            }
            return event;
        });
    }

    // ════════════════════════════════════════════════════════════════
    // Happy Path — FHIR event
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Happy path: valid FHIR event → accepted")
    class HappyPathFhir {

        @Test
        @DisplayName("Returns accepted response with event details")
        void returnsAcceptedResponse() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();

            EventIngestionResponse response = service.ingest(request);

            assertThat(response.getEventId()).isEqualTo(EVENT_ID.toString());
            assertThat(response.getCloudEventsId()).isEqualTo(CLOUD_EVENTS_ID);
            assertThat(response.getStatus()).isEqualTo("accepted");
            assertThat(response.getReceivedAt()).isNotNull();
        }

        @Test
        @DisplayName("Persists inbound event with RECEIVED then ACCEPTED status")
        void persistsWithCorrectStatusTransitions() {
            EventIngestionRequest request = buildValidFhirRequest();

            // Capture status at the time of each save (same mutable object is saved twice)
            List<InboundStatus> savedStatuses = new ArrayList<>();
            when(repository.save(any(InboundEvent.class))).thenAnswer(invocation -> {
                InboundEvent event = invocation.getArgument(0);
                savedStatuses.add(event.getStatus());
                if (event.getId() == null) {
                    event.setId(EVENT_ID);
                }
                return event;
            });

            service.ingest(request);

            assertThat(savedStatuses).hasSize(2);
            assertThat(savedStatuses.get(0)).isEqualTo(InboundStatus.RECEIVED);
            assertThat(savedStatuses.get(1)).isEqualTo(InboundStatus.ACCEPTED);
        }

        @Test
        @DisplayName("Invokes all pipeline steps in correct order")
        void invokesStepsInOrder() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();

            service.ingest(request);

            var order = inOrder(cloudEventValidator, deduplicationService,
                    repository, enricher, payloadValidator, repository, eventPublisher);
            order.verify(cloudEventValidator).validate(request);
            order.verify(deduplicationService).isDuplicate(CLOUD_EVENTS_ID, SOURCE);
            order.verify(repository).save(any(InboundEvent.class));      // Step 4: persist RECEIVED
            order.verify(enricher).enrich(eq(request), any(InboundEvent.class));
            order.verify(payloadValidator).validatePayload(request);
            order.verify(repository).save(any(InboundEvent.class));      // Step 7: persist ACCEPTED
            order.verify(eventPublisher).publish(any(EventIngestionRequest.class));
        }

        @Test
        @DisplayName("Publishes the same (enriched) request object to Kafka")
        void publishesEventToKafka() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();

            service.ingest(request);

            ArgumentCaptor<EventIngestionRequest> captor =
                    ArgumentCaptor.forClass(EventIngestionRequest.class);
            verify(eventPublisher).publish(captor.capture());
            // Same object reference — enricher mutates the request before publish
            assertThat(captor.getValue()).isSameAs(request);
            assertThat(captor.getValue().getId()).isEqualTo(request.getId());
        }

        @Test
        @DisplayName("Maps request fields correctly to InboundEvent entity")
        void mapsRequestFieldsToEntity() {
            EventIngestionRequest request = buildValidFhirRequest();
            request.setFacilityid("facility-A");
            request.setSourceeventid("src-evt-001");
            stubRepositorySave();

            service.ingest(request);

            ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
            verify(repository, org.mockito.Mockito.atLeastOnce()).save(captor.capture());
            InboundEvent saved = captor.getAllValues().get(0);

            assertThat(saved.getCloudeventsId()).isEqualTo(CLOUD_EVENTS_ID);
            assertThat(saved.getSource()).isEqualTo(SOURCE);
            assertThat(saved.getType()).isEqualTo(TYPE);
            assertThat(saved.getSpecVersion()).isEqualTo("1.0");
            assertThat(saved.getSubject()).isEqualTo(SUBJECT);
            assertThat(saved.getFacilityId()).isEqualTo("facility-A");
            assertThat(saved.getSourceEventId()).isEqualTo("src-evt-001");
            assertThat(saved.getRawPayload()).contains("\"resourceType\"");
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Happy Path — Non-FHIR JSON event
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Happy path: valid JSON event → accepted")
    class HappyPathJson {

        @Test
        @DisplayName("Returns accepted response for application/json content type")
        void returnsAcceptedForJsonContentType() {
            EventIngestionRequest request = buildValidJsonRequest();
            stubRepositorySave();

            EventIngestionResponse response = service.ingest(request);

            assertThat(response.getStatus()).isEqualTo("accepted");
            assertThat(response.getEventId()).isEqualTo(EVENT_ID.toString());
        }

        @Test
        @DisplayName("Persists raw JSON payload as-is")
        void persistsRawJsonPayload() {
            EventIngestionRequest request = buildValidJsonRequest();
            stubRepositorySave();

            service.ingest(request);

            ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
            verify(repository, org.mockito.Mockito.atLeastOnce()).save(captor.capture());
            assertThat(captor.getAllValues().get(0).getRawPayload()).contains("\"key\"");
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Invalid envelope → 400
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Invalid envelope → CloudEventValidationException")
    class InvalidEnvelope {

        @Test
        @DisplayName("Propagates CloudEventValidationException from validator")
        void propagatesValidationException() {
            EventIngestionRequest request = buildValidFhirRequest();
            doThrow(new CloudEventValidationException(List.of("id is required")))
                    .when(cloudEventValidator).validate(request);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(CloudEventValidationException.class)
                    .hasMessageContaining("id is required");
        }

        @Test
        @DisplayName("Does not persist event when envelope validation fails")
        void doesNotPersistOnEnvelopeFailure() {
            EventIngestionRequest request = buildValidFhirRequest();
            doThrow(new CloudEventValidationException(List.of("source is required")))
                    .when(cloudEventValidator).validate(request);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(CloudEventValidationException.class);

            verify(repository, never()).save(any());
        }

        @Test
        @DisplayName("Does not check dedup when envelope validation fails")
        void doesNotCheckDedupOnEnvelopeFailure() {
            EventIngestionRequest request = buildValidFhirRequest();
            doThrow(new CloudEventValidationException(List.of("type is required")))
                    .when(cloudEventValidator).validate(request);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(CloudEventValidationException.class);

            verify(deduplicationService, never()).isDuplicate(any(), any());
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Duplicate event → DuplicateEventException (→ 200)
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Duplicate event → DuplicateEventException")
    class DuplicateEvent {

        @Test
        @DisplayName("Throws DuplicateEventException with existing record ID")
        void throwsDuplicateException() {
            EventIngestionRequest request = buildValidFhirRequest();
            UUID existingId = UUID.randomUUID();
            InboundEvent existing = InboundEvent.builder()
                    .cloudeventsId(CLOUD_EVENTS_ID)
                    .source(SOURCE)
                    .build();
            existing.setId(existingId);

            when(deduplicationService.isDuplicate(CLOUD_EVENTS_ID, SOURCE)).thenReturn(true);
            when(repository.findByCloudeventsIdAndSource(CLOUD_EVENTS_ID, SOURCE))
                    .thenReturn(Optional.of(existing));

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(DuplicateEventException.class)
                    .satisfies(ex -> {
                        DuplicateEventException dup = (DuplicateEventException) ex;
                        assertThat(dup.getExistingRecordId()).isEqualTo(existingId);
                    });
        }

        @Test
        @DisplayName("Does not persist a new record for duplicates")
        void doesNotPersistDuplicate() {
            EventIngestionRequest request = buildValidFhirRequest();
            InboundEvent existing = InboundEvent.builder()
                    .cloudeventsId(CLOUD_EVENTS_ID)
                    .source(SOURCE)
                    .build();
            existing.setId(UUID.randomUUID());

            when(deduplicationService.isDuplicate(CLOUD_EVENTS_ID, SOURCE)).thenReturn(true);
            when(repository.findByCloudeventsIdAndSource(CLOUD_EVENTS_ID, SOURCE))
                    .thenReturn(Optional.of(existing));

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(DuplicateEventException.class);

            verify(repository, never()).save(any());
        }

        @Test
        @DisplayName("Does not invoke enrichment or validation for duplicates")
        void doesNotEnrichOrValidateDuplicate() {
            EventIngestionRequest request = buildValidFhirRequest();
            InboundEvent existing = InboundEvent.builder()
                    .cloudeventsId(CLOUD_EVENTS_ID)
                    .source(SOURCE)
                    .build();
            existing.setId(UUID.randomUUID());

            when(deduplicationService.isDuplicate(CLOUD_EVENTS_ID, SOURCE)).thenReturn(true);
            when(repository.findByCloudeventsIdAndSource(CLOUD_EVENTS_ID, SOURCE))
                    .thenReturn(Optional.of(existing));

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(DuplicateEventException.class);

            verify(enricher, never()).enrich(any(), any());
            verify(payloadValidator, never()).validatePayload(any());
            verify(eventPublisher, never()).publish(any());
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Invalid FHIR payload → PayloadValidationException (→ 422)
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Invalid FHIR payload → PayloadValidationException")
    class InvalidFhirPayload {

        @Test
        @DisplayName("Propagates PayloadValidationException from payload validator")
        void propagatesPayloadValidationException() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();
            doThrow(new PayloadValidationException(
                    List.of("Missing resourceType"), RejectionReason.INVALID_FHIR))
                    .when(payloadValidator).validatePayload(request);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(PayloadValidationException.class)
                    .hasMessageContaining("Missing resourceType");
        }

        @Test
        @DisplayName("Records rejection on inbound event before rethrowing")
        void recordsRejectionOnFhirFailure() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();
            PayloadValidationException fhirEx = new PayloadValidationException(
                    List.of("Invalid FHIR resource"), RejectionReason.INVALID_FHIR);
            doThrow(fhirEx).when(payloadValidator).validatePayload(request);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(PayloadValidationException.class);

            verify(rejectionService).recordRejection(
                    any(InboundEvent.class),
                    eq(RejectionReason.INVALID_FHIR),
                    eq(fhirEx.getMessage()));
        }

        @Test
        @DisplayName("Does not publish to Kafka on payload validation failure")
        void doesNotPublishOnPayloadFailure() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();
            doThrow(new PayloadValidationException(
                    List.of("Bad FHIR"), RejectionReason.INVALID_FHIR))
                    .when(payloadValidator).validatePayload(request);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(PayloadValidationException.class);

            verify(eventPublisher, never()).publish(any());
        }

        @Test
        @DisplayName("Does not update status to ACCEPTED on payload failure")
        void doesNotSetAcceptedOnPayloadFailure() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();
            doThrow(new PayloadValidationException(
                    List.of("Bad FHIR"), RejectionReason.INVALID_FHIR))
                    .when(payloadValidator).validatePayload(request);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(PayloadValidationException.class);

            // Only one save (Step 4: RECEIVED), no second save (Step 7: ACCEPTED)
            verify(repository, org.mockito.Mockito.times(1)).save(any(InboundEvent.class));
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Kafka publish failure → KafkaPublishException (→ 500)
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Kafka publish failure → KafkaPublishException")
    class KafkaPublishFailure {

        @Test
        @DisplayName("Propagates KafkaPublishException when publisher throws it")
        void propagatesKafkaPublishException() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();
            doThrow(new KafkaPublishException("cce.events.inbound", "Broker unavailable"))
                    .when(eventPublisher).publish(any(EventIngestionRequest.class));

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(KafkaPublishException.class)
                    .hasMessageContaining("Broker unavailable");
        }

        @Test
        @DisplayName("Wraps unexpected exceptions in KafkaPublishException")
        void wrapsUnexpectedExceptionInKafkaPublishException() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();
            doThrow(new RuntimeException("Connection reset"))
                    .when(eventPublisher).publish(any(EventIngestionRequest.class));

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(KafkaPublishException.class)
                    .hasMessageContaining("Connection reset");
        }

        @Test
        @DisplayName("Records rejection with KAFKA_PUBLISH_FAILURE reason")
        void recordsRejectionOnKafkaFailure() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();
            doThrow(new KafkaPublishException("cce.events.inbound", "Timeout"))
                    .when(eventPublisher).publish(any(EventIngestionRequest.class));

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(KafkaPublishException.class);

            verify(rejectionService).recordRejection(
                    any(InboundEvent.class),
                    eq(RejectionReason.KAFKA_PUBLISH_FAILURE),
                    any(String.class));
        }

        @Test
        @DisplayName("Event was ACCEPTED before Kafka failure is recorded")
        void eventWasAcceptedBeforeKafkaFailure() {
            EventIngestionRequest request = buildValidFhirRequest();

            // Track status at each save to verify RECEIVED → ACCEPTED transition
            List<InboundStatus> savedStatuses = new ArrayList<>();
            when(repository.save(any(InboundEvent.class))).thenAnswer(invocation -> {
                InboundEvent event = invocation.getArgument(0);
                savedStatuses.add(event.getStatus());
                if (event.getId() == null) {
                    event.setId(EVENT_ID);
                }
                return event;
            });

            doThrow(new KafkaPublishException("cce.events.inbound", "Timeout"))
                    .when(eventPublisher).publish(any(EventIngestionRequest.class));

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(KafkaPublishException.class);

            // Verify two saves: RECEIVED (step 4) + ACCEPTED (step 7)
            assertThat(savedStatuses).hasSize(2);
            assertThat(savedStatuses.get(0)).isEqualTo(InboundStatus.RECEIVED);
            assertThat(savedStatuses.get(1)).isEqualTo(InboundStatus.ACCEPTED);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Payload size enforcement
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Payload size enforcement")
    class PayloadSizeEnforcement {

        @Test
        @DisplayName("Rejects payload exceeding max size with VALIDATION_ERROR")
        void rejectsOversizedPayload() {
            // Build a request with a very large data payload
            StringBuilder largeJson = new StringBuilder("{\"data\":\"");
            for (int i = 0; i < MAX_PAYLOAD_SIZE + 1000; i++) {
                largeJson.append('x');
            }
            largeJson.append("\"}");

            JsonNode largeData;
            try {
                largeData = MAPPER.readTree(largeJson.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            EventIngestionRequest request = buildValidFhirRequest();
            request.setData(largeData);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(CloudEventValidationException.class)
                    .hasMessageContaining("exceeds maximum");
        }

        @Test
        @DisplayName("Allows payload within size limit")
        void allowsPayloadWithinLimit() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();

            // Default FHIR payload is well under 1 MB
            EventIngestionResponse response = service.ingest(request);
            assertThat(response.getStatus()).isEqualTo("accepted");
        }

        @Test
        @DisplayName("Does not persist event when payload too large")
        void doesNotPersistOversizedPayload() {
            StringBuilder largeJson = new StringBuilder("{\"x\":\"");
            for (int i = 0; i < MAX_PAYLOAD_SIZE + 100; i++) {
                largeJson.append('a');
            }
            largeJson.append("\"}");

            JsonNode largeData;
            try {
                largeData = MAPPER.readTree(largeJson.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            EventIngestionRequest request = buildValidFhirRequest();
            request.setData(largeData);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(CloudEventValidationException.class);

            verify(repository, never()).save(any());
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Unsupported content type
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Unsupported content type → rejection")
    class UnsupportedContentType {

        @Test
        @DisplayName("Rejects event with unsupported content type via payload validator")
        void rejectsUnsupportedContentType() {
            EventIngestionRequest request = buildValidFhirRequest();
            request.setDatacontenttype("text/xml");
            stubRepositorySave();

            doThrow(new PayloadValidationException(
                    List.of("Unsupported content type: text/xml"),
                    RejectionReason.UNSUPPORTED_CONTENT_TYPE))
                    .when(payloadValidator).validatePayload(request);

            assertThatThrownBy(() -> service.ingest(request))
                    .isInstanceOf(PayloadValidationException.class);

            verify(rejectionService).recordRejection(
                    any(InboundEvent.class),
                    eq(RejectionReason.UNSUPPORTED_CONTENT_TYPE),
                    any(String.class));
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Response field mapping
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Response field mapping")
    class ResponseMapping {

        @Test
        @DisplayName("Response contains all expected fields")
        void responseContainsAllFields() {
            EventIngestionRequest request = buildValidFhirRequest();
            stubRepositorySave();

            // Simulate enrichment setting correlationId on entity
            org.mockito.Mockito.doAnswer(invocation -> {
                InboundEvent entity = invocation.getArgument(1);
                entity.setCorrelationId(CORRELATION_ID);
                return null;
            }).when(enricher).enrich(any(), any());

            EventIngestionResponse response = service.ingest(request);

            assertThat(response.getEventId()).isNotNull();
            assertThat(response.getCloudEventsId()).isEqualTo(CLOUD_EVENTS_ID);
            assertThat(response.getStatus()).isEqualTo("accepted");
            assertThat(response.getCorrelationId()).isEqualTo(CORRELATION_ID);
            assertThat(response.getReceivedAt()).isNotNull();
        }
    }
}
