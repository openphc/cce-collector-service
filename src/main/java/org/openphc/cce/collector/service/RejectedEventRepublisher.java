package org.openphc.cce.collector.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.config.RepublishProperties;
import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventLogRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Publishes again the events whose Kafka publish failed.
 *
 * <p>Such an event is stored as REJECTED with reason KAFKA_PUBLISH_FAILURE.
 * It passed every validation. Only the publish failed. Without this job the event
 * is processed only if the source sends it again.</p>
 *
 * <p>Each run:</p>
 * <ol>
 *   <li>reads the oldest such events, up to the batch size;</li>
 *   <li>publishes each one to Kafka from its stored {@code raw_payload};</li>
 *   <li>marks it ACCEPTED when the publish worked;</li>
 *   <li>stops at the first publish failure, because Kafka is most likely still down.
 *       The next run tries again.</li>
 * </ol>
 *
 * <p>An event can be published twice. Example: two Collector pods run the job at the
 * same time, or the first publish timed out here but reached Kafka later. This is safe.
 * The Matcher skips an event it has already processed (same CloudEvents id and source).</p>
 */
@Slf4j
@Service
public class RejectedEventRepublisher {

    private final InboundEventLogRepository repository;
    private final EventDefaultsEnricher enricher;
    private final EventPublisher eventPublisher;
    private final RejectionService rejectionService;
    private final RepublishProperties properties;
    private final ObjectMapper objectMapper;
    private final Counter republishedCounter;

    public RejectedEventRepublisher(InboundEventLogRepository repository,
                                    EventDefaultsEnricher enricher,
                                    EventPublisher eventPublisher,
                                    RejectionService rejectionService,
                                    RepublishProperties properties,
                                    ObjectMapper objectMapper,
                                    MeterRegistry meterRegistry) {
        this.repository = repository;
        this.enricher = enricher;
        this.eventPublisher = eventPublisher;
        this.rejectionService = rejectionService;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.republishedCounter = Counter.builder("cce.collector.events.republished")
                .description("Events published again after a failed Kafka publish")
                .register(meterRegistry);
    }

    /**
     * Publish again the oldest events whose Kafka publish failed.
     *
     * @return how many events were published and marked ACCEPTED in this run
     */
    @Scheduled(initialDelayString = "${cce.collector.republish.interval-ms:60000}",
            fixedDelayString = "${cce.collector.republish.interval-ms:60000}")
    public int republishRejectedEvents() {
        List<InboundEventLog> rejectedEvents = repository.findByStatusAndRejectionReasonOrderByReceivedAtAsc(
                InboundStatus.REJECTED, RejectionReason.KAFKA_PUBLISH_FAILURE.name(),
                PageRequest.of(0, properties.getBatchSize()));
        if (rejectedEvents.isEmpty()) {
            return 0;
        }

        log.info("Publishing again {} event(s) whose Kafka publish failed", rejectedEvents.size());
        int republished = 0;
        for (InboundEventLog rejectedEvent : rejectedEvents) {
            if (!republish(rejectedEvent)) {
                break;
            }
            republished++;
        }
        log.info("Published again {} of {} event(s)", republished, rejectedEvents.size());
        return republished;
    }

    /**
     * Publish one stored event and mark it ACCEPTED.
     *
     * @return {@code false} when the Kafka publish failed and the run should stop
     */
    private boolean republish(InboundEventLog rejectedEvent) {
        EventIngestionRequest request;
        try {
            request = objectMapper.readValue(rejectedEvent.getRawPayload(), EventIngestionRequest.class);
        } catch (JsonProcessingException ex) {
            // The stored payload cannot be read. Give it another reason so the job leaves it alone.
            rejectionService.recordRejection(rejectedEvent, RejectionReason.DESERIALIZATION_ERROR,
                    ex.getMessage());
            return true;
        }

        try {
            keepCorrelationIdOfFirstAttempt(request, rejectedEvent);
            enricher.enrich(request, rejectedEvent);
            eventPublisher.publish(request);
        } catch (Exception ex) {
            log.warn("Could not publish again: id={}, cloudeventsId={}, error={}",
                    rejectedEvent.getId(), rejectedEvent.getCloudeventsId(), ex.getMessage());
            return false;
        }

        rejectedEvent.setStatus(InboundStatus.ACCEPTED);
        rejectedEvent.setRejectionReason(null);
        rejectedEvent.setErrorDetails(null);
        repository.save(rejectedEvent);
        republishedCounter.increment();
        log.info("Event published again and accepted: id={}, cloudeventsId={}",
                rejectedEvent.getId(), rejectedEvent.getCloudeventsId());
        return true;
    }

    /**
     * The first attempt may have generated a correlation id. It is on the row, not in the
     * stored payload. Put it on the request so the event keeps the same correlation id.
     */
    private void keepCorrelationIdOfFirstAttempt(EventIngestionRequest request, InboundEventLog rejectedEvent) {
        boolean requestHasNone = request.getCorrelationid() == null || request.getCorrelationid().isBlank();
        if (requestHasNone && rejectedEvent.getCorrelationId() != null) {
            request.setCorrelationid(rejectedEvent.getCorrelationId());
        }
    }
}
