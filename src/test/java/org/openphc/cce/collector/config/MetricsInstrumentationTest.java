package org.openphc.cce.collector.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.api.exception.CloudEventValidationException;
import org.openphc.cce.collector.api.exception.DuplicateEventException;
import org.openphc.cce.collector.api.exception.KafkaPublishException;
import org.openphc.cce.collector.config.KafkaTopicProperties;
import org.openphc.cce.collector.api.exception.PayloadValidationException;
import org.openphc.cce.collector.domain.model.InboundEvent;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventRepository;
import org.openphc.cce.collector.service.CloudEventValidator;
import org.openphc.cce.collector.service.DeduplicationService;
import org.openphc.cce.collector.service.EventDefaultsEnricher;
import org.openphc.cce.collector.service.EventIngestionService;
import org.openphc.cce.collector.service.EventPublisher;
import org.openphc.cce.collector.service.PayloadValidationResult;
import org.openphc.cce.collector.service.PayloadValidator;
import org.openphc.cce.collector.service.RejectionService;
import org.openphc.cce.collector.service.IngestionSummaryService;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Verifies that Micrometer counters and timers are correctly incremented
 * during the event ingestion pipeline.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Metrics Instrumentation")
class MetricsInstrumentationTest {

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
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final UUID EVENT_ID = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        KafkaTopicProperties kafkaTopicProperties = new KafkaTopicProperties();
        service = new EventIngestionService(
                cloudEventValidator, deduplicationService, repository,
                enricher, payloadValidator, eventPublisher, rejectionService,
                ingestionSummaryService, kafkaTopicProperties, meterRegistry, 1_048_576L);

        lenient().when(payloadValidator.validatePayload(any()))
                .thenReturn(PayloadValidationResult.builder()
                        .valid(true).errors(List.of()).build());
    }

    private EventIngestionRequest buildRequest() {
        return EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-001")
                .source("rhie-mediator")
                .type("org.openphc.encounter.created")
                .subject("UPID-12345")
                .datacontenttype("application/fhir+json")
                .data(MAPPER.valueToTree(Map.of("resourceType", "Encounter")))
                .build();
    }

    private InboundEvent buildPersistedEvent() {
        return InboundEvent.builder()
                .id(EVENT_ID)
                .cloudeventsId("evt-001")
                .source("rhie-mediator")
                .type("org.openphc.encounter.created")
                .specVersion("1.0")
                .subject("UPID-12345")
                .dataContentType("application/fhir+json")
                .rawPayload("{\"resourceType\":\"Encounter\"}")
                .status(InboundStatus.RECEIVED)
                .receivedAt(OffsetDateTime.now())
                .build();
    }

    // ════════════════════════════════════════════════════════════════
    // Received Counter
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Received Counter")
    class ReceivedCounterTests {

        @Test
        @DisplayName("increments on each ingest call")
        void incrementsOnIngest() {
            InboundEvent persisted = buildPersistedEvent();
            when(repository.save(any())).thenReturn(persisted);

            service.ingest(buildRequest());

            Counter counter = meterRegistry.find("cce.collector.events.received").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("increments even when validation fails")
        void incrementsOnFailure() {
            doThrow(new CloudEventValidationException(List.of("bad")))
                    .when(cloudEventValidator).validate(any());

            assertThatThrownBy(() -> service.ingest(buildRequest()))
                    .isInstanceOf(CloudEventValidationException.class);

            Counter counter = meterRegistry.find("cce.collector.events.received").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Accepted Counter
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Accepted Counter")
    class AcceptedCounterTests {

        @Test
        @DisplayName("increments on successful ingestion")
        void incrementsOnSuccess() {
            InboundEvent persisted = buildPersistedEvent();
            when(repository.save(any())).thenReturn(persisted);

            service.ingest(buildRequest());

            Counter counter = meterRegistry.find("cce.collector.events.accepted").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("not incremented when Kafka publish fails")
        void notIncrementedOnKafkaFailure() {
            InboundEvent persisted = buildPersistedEvent();
            when(repository.save(any())).thenReturn(persisted);
            doThrow(new KafkaPublishException("cce.events.inbound", new RuntimeException("fail")))
                    .when(eventPublisher).publish(any());

            assertThatThrownBy(() -> service.ingest(buildRequest()))
                    .isInstanceOf(KafkaPublishException.class);

            Counter counter = meterRegistry.find("cce.collector.events.accepted").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(0.0);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Duplicate Counter
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Duplicate Counter")
    class DuplicateCounterTests {

        @Test
        @DisplayName("increments when duplicate detected")
        void incrementsOnDuplicate() {
            when(deduplicationService.isDuplicate("evt-001", "rhie-mediator"))
                    .thenReturn(true);
            when(repository.findByCloudeventsIdAndSource("evt-001", "rhie-mediator"))
                    .thenReturn(Optional.of(buildPersistedEvent()));

            assertThatThrownBy(() -> service.ingest(buildRequest()))
                    .isInstanceOf(DuplicateEventException.class);

            Counter counter = meterRegistry.find("cce.collector.events.duplicate").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Rejected Counter (tagged by reason)
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Rejected Counter")
    class RejectedCounterTests {

        @Test
        @DisplayName("increments with INVALID_FHIR reason on payload failure")
        void incrementsOnPayloadFailure() {
            InboundEvent persisted = buildPersistedEvent();
            when(repository.save(any())).thenReturn(persisted);
            when(payloadValidator.validatePayload(any()))
                    .thenThrow(new PayloadValidationException(
                            List.of("bad FHIR"), RejectionReason.INVALID_FHIR));

            assertThatThrownBy(() -> service.ingest(buildRequest()))
                    .isInstanceOf(PayloadValidationException.class);

            Counter counter = meterRegistry.find("cce.collector.events.rejected")
                    .tag("reason", "INVALID_FHIR").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("increments with KAFKA_PUBLISH_FAILURE reason on Kafka failure")
        void incrementsOnKafkaFailure() {
            InboundEvent persisted = buildPersistedEvent();
            when(repository.save(any())).thenReturn(persisted);
            doThrow(new KafkaPublishException("cce.events.inbound", new RuntimeException("fail")))
                    .when(eventPublisher).publish(any());

            assertThatThrownBy(() -> service.ingest(buildRequest()))
                    .isInstanceOf(KafkaPublishException.class);

            Counter counter = meterRegistry.find("cce.collector.events.rejected")
                    .tag("reason", "KAFKA_PUBLISH_FAILURE").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1.0);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Ingestion Timer
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Ingestion Timer")
    class IngestionTimerTests {

        @Test
        @DisplayName("records duration on successful ingestion")
        void recordsDuration() {
            InboundEvent persisted = buildPersistedEvent();
            when(repository.save(any())).thenReturn(persisted);

            service.ingest(buildRequest());

            Timer timer = meterRegistry.find("cce.collector.ingestion.duration").timer();
            assertThat(timer).isNotNull();
            assertThat(timer.count()).isEqualTo(1L);
            assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                    .isGreaterThanOrEqualTo(0);
        }
    }
}
