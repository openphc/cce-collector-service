package org.openphc.cce.collector.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.api.exception.CloudEventValidationException;
import org.openphc.cce.collector.api.exception.DuplicateEventException;
import org.openphc.cce.collector.api.exception.KafkaPublishException;
import org.openphc.cce.collector.api.exception.PayloadValidationException;
import org.openphc.cce.collector.config.RepublishProperties;
import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventLogRepository;
import org.slf4j.MDC;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Tries again the stored events that were rejected because the Collector failed.
 *
 * <p>Only two rejection reasons are tried again (see {@link RejectionReason#isRetryable()}):</p>
 * <ul>
 *   <li>{@code KAFKA_PUBLISH_FAILURE}: the event was valid, but Kafka did not take it.</li>
 *   <li>{@code INTERNAL_ERROR}: something unexpected failed after the event was stored.</li>
 * </ul>
 * <p>An event rejected because it is wrong (for example INVALID_FHIR) is never tried again.</p>
 *
 * <p>The job is <strong>off by default</strong>. Turn it on with
 * {@code cce.collector.republish.enabled=true}. Example: after a Kafka outage, turn it on
 * until the waiting events are published, then turn it off.</p>
 *
 * <p>Each run:</p>
 * <ol>
 *   <li>reads the oldest such events, up to the batch size;</li>
 *   <li>sends each one through the normal ingestion steps again, from its stored
 *       {@code raw_payload}: validation, Kafka publish, then ACCEPTED;</li>
 *   <li>stops after {@value #MAX_PUBLISH_FAILURES_IN_A_ROW} Kafka failures in a row,
 *       because Kafka is most likely down. The next run tries again.</li>
 * </ol>
 *
 * <p>An event can be published twice. Example: two Collector pods run the job at the
 * same time, or the first publish timed out here but reached Kafka later. This is safe.
 * The Matcher skips an event it has already processed (same CloudEvents id and source).</p>
 *
 * <p>There is no limit on how often one event is tried. An event that fails on every run
 * is tried again on every run, for as long as the job is on.</p>
 */
@Slf4j
@Service
public class RejectedEventRepublisher {

    /** After this many Kafka failures in a row, the run stops. Kafka is most likely down. */
    static final int MAX_PUBLISH_FAILURES_IN_A_ROW = 3;

    private final InboundEventLogRepository repository;
    private final EventIngestionService eventIngestionService;
    private final RejectionService rejectionService;
    private final RepublishProperties properties;
    private final ObjectMapper objectMapper;
    private final Counter republishedCounter;

    public RejectedEventRepublisher(InboundEventLogRepository repository,
                                    EventIngestionService eventIngestionService,
                                    RejectionService rejectionService,
                                    RepublishProperties properties,
                                    ObjectMapper objectMapper,
                                    MeterRegistry meterRegistry) {
        this.repository = repository;
        this.eventIngestionService = eventIngestionService;
        this.rejectionService = rejectionService;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.republishedCounter = Counter.builder("cce.collector.events.republished")
                .description("Stored events that were tried again and accepted")
                .register(meterRegistry);
    }

    /**
     * Try again the oldest stored events that were rejected because the Collector failed.
     *
     * @return how many events were published and marked ACCEPTED in this run
     */
    @Scheduled(initialDelayString = "${cce.collector.republish.interval-ms:60000}",
            fixedDelayString = "${cce.collector.republish.interval-ms:60000}")
    public int republishRejectedEvents() {
        List<InboundEventLog> rejectedEvents = repository.findByStatusAndRejectionReasonInOrderByReceivedAtAsc(
                InboundStatus.REJECTED, RejectionReason.retryableNames(),
                PageRequest.of(0, properties.getBatchSize()));
        if (rejectedEvents.isEmpty()) {
            return 0;
        }

        log.info("Trying again {} stored event(s) that the Collector failed to process", rejectedEvents.size());
        int republished = 0;
        int publishFailuresInARow = 0;
        for (InboundEventLog rejectedEvent : rejectedEvents) {
            RetryOutcome outcome = retry(rejectedEvent);
            if (outcome == RetryOutcome.ACCEPTED) {
                republished++;
                republishedCounter.increment();
            }
            publishFailuresInARow = outcome == RetryOutcome.KAFKA_FAILED ? publishFailuresInARow + 1 : 0;
            if (publishFailuresInARow >= MAX_PUBLISH_FAILURES_IN_A_ROW) {
                log.warn("Stopping this run: {} Kafka failures in a row", publishFailuresInARow);
                break;
            }
        }
        log.info("Published {} of {} stored event(s)", republished, rejectedEvents.size());
        return republished;
    }

    /** What happened to one stored event when it was tried again. */
    private enum RetryOutcome {
        /** Published to Kafka and marked ACCEPTED. */
        ACCEPTED,
        /** Kafka did not take it. It stays REJECTED with KAFKA_PUBLISH_FAILURE and is tried again later. */
        KAFKA_FAILED,
        /** Anything else. The ingestion steps have recorded the new status of the row. */
        NOT_ACCEPTED
    }

    /**
     * Send one stored event through the ingestion steps again.
     *
     * <p>The steps record the result on the row. So this method only reports what happened.</p>
     */
    private RetryOutcome retry(InboundEventLog rejectedEvent) {
        EventIngestionRequest request;
        try {
            request = objectMapper.readValue(rejectedEvent.getRawPayload(), EventIngestionRequest.class);
        } catch (JsonProcessingException ex) {
            // The stored payload cannot be read. This reason is final, so the job leaves the row alone.
            rejectionService.recordRejection(rejectedEvent, RejectionReason.DESERIALIZATION_ERROR,
                    ex.getMessage());
            return RetryOutcome.NOT_ACCEPTED;
        }

        try {
            eventIngestionService.ingest(request);
            log.info("Stored event tried again and accepted: id={}, cloudeventsId={}",
                    rejectedEvent.getId(), rejectedEvent.getCloudeventsId());
            return RetryOutcome.ACCEPTED;
        } catch (KafkaPublishException ex) {
            log.warn("Kafka did not take the stored event: id={}, cloudeventsId={}, error={}",
                    rejectedEvent.getId(), rejectedEvent.getCloudeventsId(), ex.getMessage());
            return RetryOutcome.KAFKA_FAILED;
        } catch (CloudEventValidationException ex) {
            // The envelope check runs before the row is touched. Record the result here,
            // or the row would keep its retryable reason and be tried again on every run.
            rejectionService.recordRejection(rejectedEvent, RejectionReason.INVALID_ENVELOPE,
                    ex.getMessage());
            return RetryOutcome.NOT_ACCEPTED;
        } catch (PayloadValidationException | DuplicateEventException ex) {
            // Validation failed: the row now has that final reason.
            // Duplicate: another attempt made the row final in the meantime.
            log.info("Stored event not published: id={}, cloudeventsId={}, reason={}",
                    rejectedEvent.getId(), rejectedEvent.getCloudeventsId(), ex.getMessage());
            return RetryOutcome.NOT_ACCEPTED;
        } catch (Exception ex) {
            // The ingestion steps recorded INTERNAL_ERROR again. It is tried on the next run.
            log.warn("Stored event failed again: id={}, cloudeventsId={}, error={}",
                    rejectedEvent.getId(), rejectedEvent.getCloudeventsId(), ex.getMessage());
            return RetryOutcome.NOT_ACCEPTED;
        } finally {
            MDC.clear();
        }
    }
}
