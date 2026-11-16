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
import org.openphc.cce.collector.api.exception.KafkaPublishException;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
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
    @Mock private EventDefaultsEnricher enricher;
    @Mock private EventPublisher eventPublisher;
    @Mock private RejectionService rejectionService;

    private RepublishProperties properties;
    private SimpleMeterRegistry meterRegistry;
    private RejectedEventRepublisher republisher;

    @BeforeEach
    void setUp() {
        properties = new RepublishProperties();
        properties.setBatchSize(10);
        meterRegistry = new SimpleMeterRegistry();
        republisher = new RejectedEventRepublisher(repository, enricher, eventPublisher, rejectionService,
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
                .errorDetails("Timeout")
                .correlationId("corr-first-attempt")
                .build();
        rejectedEvent.setId(UUID.randomUUID());
        return rejectedEvent;
    }

    private void stubRejectedEvents(InboundEventLog... rejectedEvents) {
        when(repository.findByStatusAndRejectionReasonOrderByReceivedAtAsc(
                InboundStatus.REJECTED, RejectionReason.KAFKA_PUBLISH_FAILURE.name(), PageRequest.of(0, 10)))
                .thenReturn(List.of(rejectedEvents));
    }

    @Test
    @DisplayName("Publishes a stored event again and marks it ACCEPTED")
    void publishesAgainAndMarksAccepted() {
        InboundEventLog rejectedEvent = rejectedEvent("evt-1");
        stubRejectedEvents(rejectedEvent);

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isEqualTo(1);
        ArgumentCaptor<EventIngestionRequest> published = ArgumentCaptor.forClass(EventIngestionRequest.class);
        verify(eventPublisher).publish(published.capture());
        assertThat(published.getValue().getId()).isEqualTo("evt-1");
        assertThat(published.getValue().getSubject()).isEqualTo("patient-1");
        assertThat(rejectedEvent.getStatus()).isEqualTo(InboundStatus.ACCEPTED);
        assertThat(rejectedEvent.getRejectionReason()).isNull();
        assertThat(rejectedEvent.getErrorDetails()).isNull();
        verify(repository).save(rejectedEvent);
        assertThat(meterRegistry.counter("cce.collector.events.republished").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Keeps the correlation id that the first attempt generated")
    void keepsCorrelationIdOfFirstAttempt() {
        stubRejectedEvents(rejectedEvent("evt-1"));

        republisher.republishRejectedEvents();

        ArgumentCaptor<EventIngestionRequest> published = ArgumentCaptor.forClass(EventIngestionRequest.class);
        verify(eventPublisher).publish(published.capture());
        assertThat(published.getValue().getCorrelationid()).isEqualTo("corr-first-attempt");
    }

    @Test
    @DisplayName("Stops at the first publish failure and leaves the remaining events REJECTED")
    void stopsAtFirstPublishFailure() {
        InboundEventLog first = rejectedEvent("evt-1");
        InboundEventLog second = rejectedEvent("evt-2");
        InboundEventLog third = rejectedEvent("evt-3");
        stubRejectedEvents(first, second, third);
        doNothing()
                .doThrow(new KafkaPublishException("cce.events.inbound", "Broker unavailable"))
                .when(eventPublisher).publish(any(EventIngestionRequest.class));

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isEqualTo(1);
        verify(eventPublisher, times(2)).publish(any(EventIngestionRequest.class));
        assertThat(first.getStatus()).isEqualTo(InboundStatus.ACCEPTED);
        assertThat(second.getStatus()).isEqualTo(InboundStatus.REJECTED);
        assertThat(second.getRejectionReason()).isEqualTo(RejectionReason.KAFKA_PUBLISH_FAILURE.name());
        assertThat(third.getStatus()).isEqualTo(InboundStatus.REJECTED);
        verify(repository, never()).save(second);
        verify(repository, never()).save(third);
    }

    @Test
    @DisplayName("Gives an unreadable stored payload another reason and continues with the next event")
    void unreadablePayloadGetsAnotherReason() {
        InboundEventLog unreadable = rejectedEvent("evt-1");
        unreadable.setRawPayload("not json");
        InboundEventLog readable = rejectedEvent("evt-2");
        stubRejectedEvents(unreadable, readable);

        republisher.republishRejectedEvents();

        verify(rejectionService).recordRejection(eq(unreadable), eq(RejectionReason.DESERIALIZATION_ERROR),
                any(String.class));
        verify(eventPublisher, times(1)).publish(any(EventIngestionRequest.class));
        assertThat(readable.getStatus()).isEqualTo(InboundStatus.ACCEPTED);
    }

    @Test
    @DisplayName("Does nothing when no event is waiting")
    void doesNothingWhenNoEventIsWaiting() {
        stubRejectedEvents();

        int republished = republisher.republishRejectedEvents();

        assertThat(republished).isZero();
        verify(eventPublisher, never()).publish(any());
        verify(repository, never()).save(any());
    }
}
