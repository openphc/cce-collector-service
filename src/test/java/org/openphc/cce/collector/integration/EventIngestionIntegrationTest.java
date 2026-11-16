package org.openphc.cce.collector.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.api.exception.KafkaPublishException;
import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.service.RejectedEventRepublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Full pipeline integration tests for the CCE Collector Service.
 *
 * <p>Uses H2 in-memory database and mocked {@code InboundEventProducer}
 * to verify the complete event ingestion flow end-to-end via {@code MockMvc}.</p>
 */
@DisplayName("Event Ingestion Integration Tests")
class EventIngestionIntegrationTest extends AbstractIntegrationTest {

    private static final String EVENTS_URL = "/v1/events";
    private static final MediaType JSON = MediaType.APPLICATION_JSON;

    @Autowired
    private RejectedEventRepublisher rejectedEventRepublisher;

    @BeforeEach
    void setUp() {
        inboundEventRepository.deleteAll();
        reset(inboundEventProducer);
    }

    // ═══════════════════════════════════════════════════════════════
    // Helper methods
    // ═══════════════════════════════════════════════════════════════

    /**
     * Builds a valid FHIR Encounter event request JSON string.
     */
    private String validFhirEncounterJson() {
        return """
            {
              "specversion": "1.0",
              "id": "evt-001",
              "source": "urn:openhim:test",
              "type": "org.openphc.encounter",
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
            }
            """;
    }

    /**
     * Builds a valid FHIR Observation event request JSON string.
     */
    private String validFhirObservationJson() {
        return """
            {
              "specversion": "1.0",
              "id": "evt-obs-001",
              "source": "urn:openhim:test",
              "type": "org.openphc.observation",
              "subject": "UPID-12345",
              "datacontenttype": "application/fhir+json",
              "data": {
                "resourceType": "Observation",
                "id": "obs-001",
                "status": "final",
                "code": {
                  "coding": [
                    {
                      "system": "http://loinc.org",
                      "code": "85354-9",
                      "display": "Blood pressure"
                    }
                  ]
                },
                "subject": {
                  "reference": "Patient/UPID-12345"
                }
              }
            }
            """;
    }

    /**
     * Builds a valid non-FHIR JSON event request string.
     */
    private String validPlainJsonEvent() {
        return """
            {
              "specversion": "1.0",
              "id": "evt-json-001",
              "source": "urn:openhim:test",
              "type": "org.openphc.custom",
              "subject": "UPID-12345",
              "datacontenttype": "application/json",
              "data": {
                "key": "value",
                "nested": { "count": 42 }
              }
            }
            """;
    }

    /**
     * Builds a minimal valid request with overridable fields.
     */
    private EventIngestionRequest.EventIngestionRequestBuilder validRequestBuilder() {
        ObjectNode data = JsonNodeFactory.instance.objectNode();
        data.put("resourceType", "Encounter");
        data.put("id", "enc-001");
        data.put("status", "finished");
        ObjectNode classNode = data.putObject("class");
        classNode.put("system", "http://terminology.hl7.org/CodeSystem/v3-ActCode");
        classNode.put("code", "AMB");
        ObjectNode subjectNode = data.putObject("subject");
        subjectNode.put("reference", "Patient/UPID-12345");

        return EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-001")
                .source("urn:openhim:test")
                .type("org.openphc.encounter")
                .subject("UPID-12345")
                .datacontenttype("application/fhir+json")
                .data(data);
    }

    // ═══════════════════════════════════════════════════════════════
    // 1. Happy Path Tests
    // ═══════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Happy Path — Valid Events")
    class HappyPath {

        @Test
        @DisplayName("Valid FHIR Encounter → 202, ACCEPTED, Kafka publish called")
        void validFhirEncounter_returns202() throws Exception {
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.data.eventId").isNotEmpty())
                    .andExpect(jsonPath("$.data.cloudEventsId").value("evt-001"))
                    .andExpect(jsonPath("$.data.status").value("accepted"))
                    .andExpect(jsonPath("$.data.correlationId").isNotEmpty())
                    .andExpect(jsonPath("$.data.receivedAt").isNotEmpty());

            // Verify Kafka publish was called
            verify(inboundEventProducer, times(1)).publish(any(EventIngestionRequest.class));

            // Verify DB state
            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.ACCEPTED);
            assertThat(events.get(0).getCloudeventsId()).isEqualTo("evt-001");
        }

        @Test
        @DisplayName("Valid FHIR Observation → 202, ACCEPTED")
        void validFhirObservation_returns202() throws Exception {
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirObservationJson()))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.data.status").value("accepted"));

            verify(inboundEventProducer, times(1)).publish(any(EventIngestionRequest.class));

            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.ACCEPTED);
        }

        @Test
        @DisplayName("Valid non-FHIR JSON → 202, ACCEPTED, no FHIR validation")
        void validPlainJson_returns202() throws Exception {
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validPlainJsonEvent()))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.data.status").value("accepted"));

            verify(inboundEventProducer, times(1)).publish(any(EventIngestionRequest.class));

            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.ACCEPTED);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 2. Validation Failure Tests
    // ═══════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Validation Failures")
    class ValidationFailures {

        @Test
        @DisplayName("Missing specversion → 400")
        void missingSpecversion_returns400() throws Exception {
            String json = """
                {
                  "id": "evt-001",
                  "source": "urn:openhim:test",
                  "type": "org.openphc.encounter",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/fhir+json",
                  "data": { "resourceType": "Encounter", "id": "e1", "status": "finished",
                    "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB"},
                    "subject": {"reference": "Patient/UPID-12345"} }
                }
                """;

            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(json))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error.code").value("VALIDATION_ERROR"))
                    .andExpect(jsonPath("$.error.message").isNotEmpty());

            verify(inboundEventProducer, never()).publish(any());
        }

        @Test
        @DisplayName("Missing subject → 400")
        void missingSubject_returns400() throws Exception {
            String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-001",
                  "source": "urn:openhim:test",
                  "type": "org.openphc.encounter",
                  "datacontenttype": "application/fhir+json",
                  "data": { "resourceType": "Encounter", "id": "e1", "status": "finished",
                    "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB"},
                    "subject": {"reference": "Patient/UPID-12345"} }
                }
                """;

            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(json))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error.code").value("VALIDATION_ERROR"))
                    .andExpect(jsonPath("$.error.message", containsString("subject")));

            verify(inboundEventProducer, never()).publish(any());
        }

        @Test
        @DisplayName("Missing type → 400")
        void missingType_returns400() throws Exception {
            String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-001",
                  "source": "urn:openhim:test",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/fhir+json",
                  "data": { "resourceType": "Encounter", "id": "e1", "status": "finished",
                    "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB"},
                    "subject": {"reference": "Patient/UPID-12345"} }
                }
                """;

            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(json))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error.code").value("VALIDATION_ERROR"))
                    .andExpect(jsonPath("$.error.message", containsString("type")));

            verify(inboundEventProducer, never()).publish(any());
        }

        @Test
        @DisplayName("Invalid FHIR payload (missing resourceType) → 422, REJECTED")
        void invalidFhir_returns422() throws Exception {
            String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-fhir-bad",
                  "source": "urn:openhim:test",
                  "type": "org.openphc.encounter",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/fhir+json",
                  "data": { "someField": "notAFhirResource" }
                }
                """;

            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(json))
                    .andExpect(status().isUnprocessableEntity())
                    .andExpect(jsonPath("$.error.code").value("PAYLOAD_VALIDATION_ERROR"));

            verify(inboundEventProducer, never()).publish(any());

            // Verify DB state — event should be REJECTED
            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.REJECTED);
            assertThat(events.get(0).getRejectionReason()).isEqualTo("INVALID_FHIR");
        }

        @Test
        @DisplayName("Invalid non-FHIR JSON (empty data) → 422, REJECTED")
        void invalidJson_returns422() throws Exception {
            String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-json-bad",
                  "source": "urn:openhim:test",
                  "type": "org.openphc.custom",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/json",
                  "data": {}
                }
                """;

            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(json))
                    .andExpect(status().isUnprocessableEntity())
                    .andExpect(jsonPath("$.error.code").value("PAYLOAD_VALIDATION_ERROR"));

            verify(inboundEventProducer, never()).publish(any());

            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.REJECTED);
            assertThat(events.get(0).getRejectionReason()).isEqualTo("INVALID_JSON");
        }

        @Test
        @DisplayName("Unsupported datacontenttype → 422, REJECTED")
        void unsupportedContentType_returns422() throws Exception {
            String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-xml",
                  "source": "urn:openhim:test",
                  "type": "org.openphc.encounter",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/xml",
                  "data": { "some": "data" }
                }
                """;

            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(json))
                    .andExpect(status().isUnprocessableEntity())
                    .andExpect(jsonPath("$.error.code").value("PAYLOAD_VALIDATION_ERROR"));

            verify(inboundEventProducer, never()).publish(any());

            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.REJECTED);
            assertThat(events.get(0).getRejectionReason()).isEqualTo("UNSUPPORTED_CONTENT_TYPE");
        }

        @Test
        @DisplayName("Oversized payload → 400")
        void oversizedPayload_returns400() throws Exception {
            // Build a payload larger than default 1 MB
            ObjectNode data = JsonNodeFactory.instance.objectNode();
            data.put("resourceType", "Encounter");
            data.put("id", "enc-big");
            data.put("status", "finished");
            ObjectNode classNode = data.putObject("class");
            classNode.put("system", "http://terminology.hl7.org/CodeSystem/v3-ActCode");
            classNode.put("code", "AMB");
            // Add a large string to exceed 1 MB
            data.put("bigField", "x".repeat(1_100_000));

            EventIngestionRequest request = EventIngestionRequest.builder()
                    .specversion("1.0")
                    .id("evt-big")
                    .source("urn:openhim:test")
                    .type("org.openphc.encounter")
                    .subject("UPID-12345")
                    .datacontenttype("application/fhir+json")
                    .data(data)
                    .build();

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error.code").value("VALIDATION_ERROR"));

            verify(inboundEventProducer, never()).publish(any());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 3. Deduplication Tests
    // ═══════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Deduplication")
    class Deduplication {

        @Test
        @DisplayName("Same (id, source) submitted twice → first 202, second 200 (duplicate)")
        void duplicateEvent_returns200() throws Exception {
            // First submission → 202
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.data.status").value("accepted"));

            // Second submission with same id + source → 200
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.data.status").value("duplicate"))
                    .andExpect(jsonPath("$.data.eventId").isNotEmpty());
        }

        @Test
        @DisplayName("Same id, different source → both 202 (not duplicate)")
        void sameIdDifferentSource_bothAccepted() throws Exception {
            // First event
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted());

            // Second event: same id but different source
            String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-001",
                  "source": "urn:openhim:different-source",
                  "type": "org.openphc.encounter",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/fhir+json",
                  "data": {
                    "resourceType": "Encounter",
                    "id": "enc-002",
                    "status": "finished",
                    "class": {
                      "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                      "code": "AMB"
                    },
                    "subject": {
                      "reference": "Patient/UPID-12345"
                    }
                  }
                }
                """;

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(json))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.data.status").value("accepted"));

            assertThat(inboundEventRepository.findAll()).hasSize(2);
        }

        @Test
        @DisplayName("KafkaTemplate.send() NOT called for duplicate")
        void duplicate_kafkaNotCalled() throws Exception {
            // First submission
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted());

            verify(inboundEventProducer, times(1)).publish(any(EventIngestionRequest.class));
            reset(inboundEventProducer);

            // Second (duplicate) submission
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isOk());

            // Kafka producer should NOT be called for duplicate
            verify(inboundEventProducer, never()).publish(any());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 4. Kafka Failure Tests
    // ═══════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Kafka Failure")
    class KafkaFailure {

        @Test
        @DisplayName("Kafka publish failure → 500, inbound_event_log REJECTED")
        void kafkaFailure_returns500_eventRejected() throws Exception {
            doThrow(new KafkaPublishException("cce.events.inbound",
                    new RuntimeException("Broker unavailable")))
                    .when(inboundEventProducer).publish(any(EventIngestionRequest.class));

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.error.code").value("EVENT_PUBLISH_FAILURE"))
                    .andExpect(jsonPath("$.error.message").isNotEmpty());

            // Verify DB state — event should be REJECTED with KAFKA_PUBLISH_FAILURE
            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.REJECTED);
            assertThat(events.get(0).getRejectionReason()).isEqualTo("KAFKA_PUBLISH_FAILURE");
        }

        @Test
        @DisplayName("Retry after a Kafka publish failure → 202 accepted, published, same row ACCEPTED")
        void retryAfterKafkaFailure_isAcceptedAndPublished() throws Exception {
            // First attempt: Kafka is down. Second attempt: Kafka is back.
            doThrow(new KafkaPublishException("cce.events.inbound",
                    new RuntimeException("Broker unavailable")))
                    .doNothing()
                    .when(inboundEventProducer).publish(any(EventIngestionRequest.class));

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isInternalServerError());

            // The source sends the same event again. It must not be answered "duplicate".
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.data.status").value("accepted"));

            verify(inboundEventProducer, times(2)).publish(any(EventIngestionRequest.class));
            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.ACCEPTED);
            assertThat(events.get(0).getRejectionReason()).isNull();

            // A third attempt is a real duplicate now.
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.data.status").value("duplicate"));
            verify(inboundEventProducer, times(2)).publish(any(EventIngestionRequest.class));
        }

        @Test
        @DisplayName("Retry of an event rejected by validation → 200 duplicate, not processed again")
        void retryOfInvalidEvent_isFinal() throws Exception {
            String invalidFhir = """
                {
                  "specversion": "1.0",
                  "id": "evt-fhir-bad",
                  "source": "urn:openhim:test",
                  "type": "org.openphc.encounter",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/fhir+json",
                  "data": { "someField": "notAFhirResource" }
                }
                """;
            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(invalidFhir))
                    .andExpect(status().isUnprocessableEntity());

            // The event is wrong. Sending it again cannot help, so the rejection is final.
            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(invalidFhir))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.data.status").value("duplicate"));

            verify(inboundEventProducer, never()).publish(any());
            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.REJECTED);
            assertThat(events.get(0).getRejectionReason()).isEqualTo("INVALID_FHIR");
        }

        @Test
        @DisplayName("Retry of an event rejected with INTERNAL_ERROR → 202 accepted, published")
        void retryAfterInternalError_isAcceptedAndPublished() throws Exception {
            storeRejected("evt-001", "INTERNAL_ERROR", validFhirEncounterJson());

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.data.status").value("accepted"));

            verify(inboundEventProducer, times(1)).publish(any(EventIngestionRequest.class));
            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.ACCEPTED);
            assertThat(events.get(0).getRejectionReason()).isNull();
        }

        @Test
        @DisplayName("The republish job tries INTERNAL_ERROR and KAFKA_PUBLISH_FAILURE events, and leaves INVALID_FHIR alone")
        void republishJob_triesOnlyRetryableReasons() throws Exception {
            storeRejected("evt-001", "INTERNAL_ERROR", validFhirEncounterJson());
            storeRejected("evt-002", "KAFKA_PUBLISH_FAILURE", validFhirEncounterJson().replace("evt-001", "evt-002"));
            storeRejected("evt-003", "INVALID_FHIR", validFhirEncounterJson().replace("evt-001", "evt-003"));

            int republished = rejectedEventRepublisher.republishRejectedEvents();

            assertThat(republished).isEqualTo(2);
            verify(inboundEventProducer, times(2)).publish(any(EventIngestionRequest.class));
            assertThat(statusOf("evt-001")).isEqualTo(InboundStatus.ACCEPTED);
            assertThat(statusOf("evt-002")).isEqualTo(InboundStatus.ACCEPTED);
            assertThat(statusOf("evt-003")).isEqualTo(InboundStatus.REJECTED);
        }

        @Test
        @DisplayName("The republish job validates again: a stored INTERNAL_ERROR event that is not valid FHIR becomes INVALID_FHIR")
        void republishJob_validatesAgain() throws Exception {
            String invalidFhir = validFhirEncounterJson().replace("\"resourceType\": \"Encounter\"", "\"someField\": \"x\"");
            assertThat(invalidFhir).doesNotContain("resourceType");
            storeRejected("evt-001", "INTERNAL_ERROR", invalidFhir);

            int republished = rejectedEventRepublisher.republishRejectedEvents();

            assertThat(republished).isZero();
            verify(inboundEventProducer, never()).publish(any());
            InboundEventLog event = inboundEventRepository.findByCloudeventsIdAndSource("evt-001", "urn:openhim:test")
                    .orElseThrow();
            assertThat(event.getStatus()).isEqualTo(InboundStatus.REJECTED);
            assertThat(event.getRejectionReason()).isEqualTo("INVALID_FHIR");

            // The reason is final now. The next run leaves the event alone.
            assertThat(rejectedEventRepublisher.republishRejectedEvents()).isZero();
        }

        private void storeRejected(String cloudeventsId, String rejectionReason, String rawPayload) {
            inboundEventRepository.save(InboundEventLog.builder()
                    .cloudeventsId(cloudeventsId)
                    .source("urn:openhim:test")
                    .rawPayload(rawPayload)
                    .status(InboundStatus.REJECTED)
                    .rejectionReason(rejectionReason)
                    .build());
        }

        private InboundStatus statusOf(String cloudeventsId) {
            return inboundEventRepository.findByCloudeventsIdAndSource(cloudeventsId, "urn:openhim:test")
                    .orElseThrow().getStatus();
        }

        @Test
        @DisplayName("The republish job publishes a stored REJECTED event and marks it ACCEPTED")
        void republishJob_publishesStoredRejectedEvent() throws Exception {
            doThrow(new KafkaPublishException("cce.events.inbound",
                    new RuntimeException("Broker unavailable")))
                    .doNothing()
                    .when(inboundEventProducer).publish(any(EventIngestionRequest.class));

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isInternalServerError());

            int republished = rejectedEventRepublisher.republishRejectedEvents();

            assertThat(republished).isEqualTo(1);
            ArgumentCaptor<EventIngestionRequest> published = ArgumentCaptor.forClass(EventIngestionRequest.class);
            verify(inboundEventProducer, times(2)).publish(published.capture());
            assertThat(published.getValue().getId()).isEqualTo("evt-001");
            assertThat(published.getValue().getSubject()).isEqualTo("UPID-12345");
            assertThat(published.getValue().getData()).isEqualTo(published.getAllValues().get(0).getData());
            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getStatus()).isEqualTo(InboundStatus.ACCEPTED);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 5. Field Mapping Verification
    // ═══════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Field Mapping Verification")
    class FieldMapping {

        @Test
        @DisplayName("Kafka message key equals subject")
        void kafkaMessageKey_equalsSubject() throws Exception {
            ArgumentCaptor<EventIngestionRequest> captor =
                    ArgumentCaptor.forClass(EventIngestionRequest.class);

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted());

            verify(inboundEventProducer).publish(captor.capture());

            EventIngestionRequest captured = captor.getValue();
            // subject is used as Kafka key in InboundEventProducer
            assertThat(captured.getSubject()).isEqualTo("UPID-12345");
        }

        @Test
        @DisplayName("Kafka message fields are lowercase (CloudEvents spec)")
        void kafkaMessageFields_areLowercase() throws Exception {
            ArgumentCaptor<EventIngestionRequest> captor =
                    ArgumentCaptor.forClass(EventIngestionRequest.class);

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted());

            verify(inboundEventProducer).publish(captor.capture());

            EventIngestionRequest captured = captor.getValue();
            // Verify the DTO field names are lowercase (Jackson serialization
            // will use these getters which are named after the lowercase fields)
            String serialized = objectMapper.writeValueAsString(captured);
            assertThat(serialized).contains("\"specversion\"");
            assertThat(serialized).contains("\"datacontenttype\"");
            assertThat(serialized).contains("\"correlationid\"");
            assertThat(serialized).doesNotContain("\"specVersion\"");
            assertThat(serialized).doesNotContain("\"dataContentType\"");
            assertThat(serialized).doesNotContain("\"correlationId\"");
        }

        @Test
        @DisplayName("correlationid is generated when absent")
        void correlationId_generated() throws Exception {
            ArgumentCaptor<EventIngestionRequest> captor =
                    ArgumentCaptor.forClass(EventIngestionRequest.class);

            // Request without correlationid
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted());

            verify(inboundEventProducer).publish(captor.capture());

            EventIngestionRequest captured = captor.getValue();
            assertThat(captured.getCorrelationid()).isNotNull();
            assertThat(captured.getCorrelationid()).startsWith("corr-");
        }

        @Test
        @DisplayName("correlationid preserved when provided by source")
        void correlationId_preserved() throws Exception {
            String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-corr",
                  "source": "urn:openhim:test",
                  "type": "org.openphc.encounter",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/fhir+json",
                  "correlationid": "source-correlation-123",
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
                }
                """;

            ArgumentCaptor<EventIngestionRequest> captor =
                    ArgumentCaptor.forClass(EventIngestionRequest.class);

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(json))
                    .andExpect(status().isAccepted());

            verify(inboundEventProducer).publish(captor.capture());

            EventIngestionRequest captured = captor.getValue();
            assertThat(captured.getCorrelationid()).isEqualTo("source-correlation-123");
        }

        @Test
        @DisplayName("type is passed through unchanged (no normalization)")
        void type_passedThrough() throws Exception {
            String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-type",
                  "source": "urn:openhim:test",
                  "type": "some.arbitrary.custom.type",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/json",
                  "data": { "key": "value" }
                }
                """;

            ArgumentCaptor<EventIngestionRequest> captor =
                    ArgumentCaptor.forClass(EventIngestionRequest.class);

            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(json))
                    .andExpect(status().isAccepted());

            verify(inboundEventProducer).publish(captor.capture());

            EventIngestionRequest captured = captor.getValue();
            assertThat(captured.getType()).isEqualTo("some.arbitrary.custom.type");
        }

        @Test
        @DisplayName("DB entity fields map correctly from request")
        void dbEntityFieldMapping() throws Exception {
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted());

            List<InboundEventLog> events = inboundEventRepository.findAll();
            assertThat(events).hasSize(1);

            InboundEventLog event = events.get(0);
            assertThat(event.getId()).isNotNull();
            assertThat(event.getCloudeventsId()).isEqualTo("evt-001");
            assertThat(event.getSource()).isEqualTo("urn:openhim:test");
            assertThat(event.getStatus()).isEqualTo(InboundStatus.ACCEPTED);
            assertThat(event.getCorrelationId()).startsWith("corr-");
            assertThat(event.getReceivedAt()).isNotNull();
            assertThat(event.getRawPayload()).isNotEmpty();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 6. Response Envelope Verification
    // ═══════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Response Envelopes")
    class ResponseEnvelopes {

        @Test
        @DisplayName("Success response wrapped in ApiResponse {data: ...}")
        void successResponse_wrappedInApiResponse() throws Exception {
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.data").exists())
                    .andExpect(jsonPath("$.data.eventId").isNotEmpty())
                    .andExpect(jsonPath("$.data.cloudEventsId").isNotEmpty())
                    .andExpect(jsonPath("$.data.status").isNotEmpty())
                    .andExpect(jsonPath("$.error").doesNotExist());
        }

        @Test
        @DisplayName("Error response wrapped in ApiError {error: {code, message}}")
        void errorResponse_wrappedInApiError() throws Exception {
            String json = """
                {
                  "id": "evt-bad",
                  "source": "urn:openhim:test",
                  "type": "test",
                  "subject": "UPID-12345",
                  "datacontenttype": "application/json",
                  "data": { "key": "value" }
                }
                """;

            mockMvc.perform(post(EVENTS_URL).contentType(JSON).content(json))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error").exists())
                    .andExpect(jsonPath("$.error.code").isNotEmpty())
                    .andExpect(jsonPath("$.error.message").isNotEmpty())
                    .andExpect(jsonPath("$.data").doesNotExist());
        }

        @Test
        @DisplayName("Duplicate response wrapped in ApiResponse {data: ...} with status=duplicate")
        void duplicateResponse_wrappedInApiResponse() throws Exception {
            // First submission
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isAccepted());

            // Duplicate
            mockMvc.perform(post(EVENTS_URL)
                            .contentType(JSON)
                            .content(validFhirEncounterJson()))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.data").exists())
                    .andExpect(jsonPath("$.data.status").value("duplicate"))
                    .andExpect(jsonPath("$.error").doesNotExist());
        }
    }
}
