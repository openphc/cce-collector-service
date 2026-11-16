package org.openphc.cce.collector.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.api.exception.CloudEventValidationException;
import org.openphc.cce.collector.api.exception.KafkaPublishException;
import org.openphc.cce.collector.api.exception.PayloadValidationException;
import org.openphc.cce.collector.config.RepublishProperties;
import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventLogRepository;
import org.springframework.data.domain.PageRequest;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RejectedEventRepublisher}.
 */
@ExtendWith(MockitoExtension.class)
class RejectedEventRepublisherTest {

    private static final String SOURCE = "rhie-mediator";

    @Mock private InboundEventLogRepository repository;
    @Mock private EventIngestionService eventIngestionService;
    @Mock private RejectionService rejectionService;

    private SimpleMeterRegistry meterRegistry;
    private RejectedEventRepublisher republisher;

    @BeforeEach
    void setUp() {
        RepublishProperties properties = new RepublishProperties();
        properties.setBatchSize(10);
        meterRegistry = new SimpleMeterRegistry();
        republisher = new RejectedEventRepublisher(repository, eventIngestionService, rejectionService,
                properties, new ObjectMapper(), meterRegistry);
    }

    private InboundEventLog rejectedEvent(String cloudeventsId) {
        InboundEventLog rejectedEvent = InboundEventLog.builder()
                .cloudeventsId(cloudeventsId)
                .source(SOURCE)
                .rawPayload("{\"specversion\":\"1.0\",\"id\":\"" + cloudeventsId + "\",\"source\":\"" + SOURCE
                        + "\",\"type\":\"org.openphc.encounter.created\",\"subject\":\"patient-1\"}")
                .status(InboundStatus.REJECTED)
                .rejectionReason(RejectionReason.KAFKA_PUBLISH_FAILURE.name())
                .build();
        rejectedEvent.setId(UUID.randomUUID());
        return rejectedEvent;
    }

    private void stubRejectedEvents(InboundEventLog... rejectedEvents) {
        when(repository.findByStatusAndRejectionReasonInOrderByReceivedAtAsc(
                InboundStatus.REJECTED, List.of("KAFKA_PUBLISH_FAILURE", "INTERNAL_ERROR"), PageRequest.of(0, 10)))
                .thenReturn(List.of(rejectedEvents));
    }

    private EventIngestionRequest requestWithId(String cloudeventsId) {
        return argThat(request -> request != null && cloudeventsId.equals(request.getId()));
    }

    @Test
    @DisplayName("Reads only REJECTED events with KAFKA_PUBLISH_FAILURE or INTERNAL_ERROR")
    void readsOnlyRetryableReasons() {
        stubRejectedEvents();

        republisher.republishRejectedEvents();

        verify(repository).findByStatusAndRejectionReasonInOrderByReceivedAtAsc(
                InboundStatus.REJECTED, List.of("KAFKA_PUBLISH_FAILURE", "INTERNAL_ERROR"), PageRequest.of(0, 10));
    }

    @Test
    @DisplayName("Sends the stored payload through the ingestion steps again")
    void sendsStoredPayloadThroughIngestionAgain() {
        stubRejectedEvents(rejectedEvent("evt-1"));

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isEqualTo(1);
        ArgumentCaptor<EventIngestionRequest> ingested = ArgumentCaptor.forClass(EventIngestionRequest.class);
        verify(eventIngestionService).ingest(ingested.capture());
        assertThat(ingested.getValue().getId()).isEqualTo("evt-1");
        assertThat(ingested.getValue().getSource()).isEqualTo(SOURCE);
        assertThat(ingested.getValue().getSubject()).isEqualTo("patient-1");
        assertThat(meterRegistry.counter("cce.collector.events.republished").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("One Kafka failure does not stop the run: the next events are still tried")
    void oneKafkaFailureDoesNotStopTheRun() {
        stubRejectedEvents(rejectedEvent("evt-1"), rejectedEvent("evt-2"), rejectedEvent("evt-3"));
        doThrow(new KafkaPublishException("cce.events.inbound", "Record too large"))
                .when(eventIngestionService).ingest(requestWithId("evt-1"));

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isEqualTo(2);
        verify(eventIngestionService, times(3)).ingest(any(EventIngestionRequest.class));
    }

    @Test
    @DisplayName("Three Kafka failures in a row stop the run: Kafka is most likely down")
    void threeKafkaFailuresInARowStopTheRun() {
        stubRejectedEvents(rejectedEvent("evt-1"), rejectedEvent("evt-2"), rejectedEvent("evt-3"),
                rejectedEvent("evt-4"), rejectedEvent("evt-5"));
        doThrow(new KafkaPublishException("cce.events.inbound", "Broker unavailable"))
                .when(eventIngestionService).ingest(any(EventIngestionRequest.class));

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isZero();
        verify(eventIngestionService, times(3)).ingest(any(EventIngestionRequest.class));
        verify(eventIngestionService, never()).ingest(requestWithId("evt-4"));
    }

    @Test
    @DisplayName("An event that fails validation now is not counted, and the run goes on")
    void validationFailureIsNotCountedAndTheRunGoesOn() {
        stubRejectedEvents(rejectedEvent("evt-1"), rejectedEvent("evt-2"));
        doThrow(new PayloadValidationException(List.of("not FHIR"), RejectionReason.INVALID_FHIR))
                .when(eventIngestionService).ingest(requestWithId("evt-1"));

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isEqualTo(1);
        verify(eventIngestionService, times(2)).ingest(any(EventIngestionRequest.class));
    }

    @Test
    @DisplayName("An event that fails the envelope check gets INVALID_ENVELOPE, so it is not tried again")
    void envelopeFailureGetsAFinalReason() {
        InboundEventLog rejectedEvent = rejectedEvent("evt-1");
        stubRejectedEvents(rejectedEvent);
        doThrow(new CloudEventValidationException(List.of("Payload too large")))
                .when(eventIngestionService).ingest(any(EventIngestionRequest.class));

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isZero();
        verify(rejectionService).recordRejection(eq(rejectedEvent), eq(RejectionReason.INVALID_ENVELOPE),
                any(String.class));
    }

    @Test
    @DisplayName("An unexpected failure does not stop the run")
    void unexpectedFailureDoesNotStopTheRun() {
        stubRejectedEvents(rejectedEvent("evt-1"), rejectedEvent("evt-2"));
        doThrow(new IllegalStateException("boom"))
                .when(eventIngestionService).ingest(requestWithId("evt-1"));

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isEqualTo(1);
    }

    @Test
    @DisplayName("An unreadable stored payload gets DESERIALIZATION_ERROR, and the run goes on")
    void unreadablePayloadGetsAFinalReason() {
        InboundEventLog unreadable = rejectedEvent("evt-1");
        unreadable.setRawPayload("not json");
        stubRejectedEvents(unreadable, rejectedEvent("evt-2"));

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isEqualTo(1);
        verify(rejectionService).recordRejection(eq(unreadable), eq(RejectionReason.DESERIALIZATION_ERROR),
                any(String.class));
        verify(eventIngestionService, times(1)).ingest(any(EventIngestionRequest.class));
    }

    @Test
    @DisplayName("Does nothing when no event is waiting")
    void doesNothingWhenNoEventIsWaiting() {
        stubRejectedEvents();

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isZero();
        verify(eventIngestionService, never()).ingest(any());
    }
}
