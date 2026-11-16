package org.openphc.cce.collector.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.api.dto.EventIngestionResponse;
import org.openphc.cce.collector.api.exception.CloudEventValidationException;
import org.openphc.cce.collector.api.exception.DuplicateEventException;
import org.openphc.cce.collector.api.exception.KafkaPublishException;
import org.openphc.cce.collector.api.exception.PayloadValidationException;
import org.openphc.cce.common.kafka.KafkaTopicProperties;
import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Main event ingestion orchestrator implementing the 8-step processing pipeline.
 *
 * <h3>Pipeline Steps</h3>
 * <ol>
 *   <li>Receive HTTP POST → parse request body (handled by Spring MVC)</li>
 *   <li>CloudEvents envelope validation → 400 on failure</li>
 *   <li>Deduplication check → 200 (idempotent) if duplicate</li>
 *   <li>Persist to {@code inbound_event_log} (status = RECEIVED) — raw request stored as-is</li>
 *   <li>Apply server-side defaults (correlationid, time)</li>
 *   <li>Payload validation (branched by datacontenttype) → 422 on failure</li>
 *   <li>Synchronous Kafka publish → 500 on failure</li>
 *   <li>Update {@code inbound_event_log.status} = ACCEPTED</li>
 * </ol>
 *
 * <p>ACCEPTED means the event is in Kafka. When the source sends an event again:</p>
 * <ul>
 *   <li>the earlier attempt was accepted, or was rejected because the event is wrong
 *       → "duplicate". Nothing is processed.</li>
 *   <li>the earlier attempt was rejected because the Collector failed
 *       (KAFKA_PUBLISH_FAILURE, INTERNAL_ERROR), or is still RECEIVED
 *       → the event is processed again, on the same row.</li>
 * </ul>
 * <p>This is how a failed Kafka publish is repaired by the source's retry.</p>
 *
 * <p>Each failure case updates the {@code inbound_event_log} record with the
 * appropriate {@link RejectionReason} (except envelope failures which occur
 * before persistence).</p>
 *
 * <p>No {@code @Transactional} annotation — each {@code repository.save()}
 * auto-commits in its own transaction. This ensures the initial persist
 * (Step 4) is committed <strong>before</strong> Kafka publish (Step 7),
 * satisfying the transaction boundary requirement.</p>
 */
@Slf4j
@Service
public class EventIngestionService {

    private final CloudEventValidator cloudEventValidator;
    private final DeduplicationService deduplicationService;
    private final InboundEventLogRepository repository;
    private final EventDefaultsEnricher enricher;
    private final PayloadValidator payloadValidator;
    private final EventPublisher eventPublisher;
    private final RejectionService rejectionService;
    private final KafkaTopicProperties kafkaTopicProperties;
    private final ObjectMapper objectMapper;
    private final long maxPayloadSize;

    // ── Metrics ────────────────────────────────────────────────
    private final Counter receivedCounter;
    private final Counter acceptedCounter;
    private final Counter duplicateCounter;
    private final MeterRegistry meterRegistry;
    private final Timer ingestionTimer;

    public EventIngestionService(
            CloudEventValidator cloudEventValidator,
            DeduplicationService deduplicationService,
            InboundEventLogRepository repository,
            EventDefaultsEnricher enricher,
            PayloadValidator payloadValidator,
            EventPublisher eventPublisher,
            RejectionService rejectionService,
            KafkaTopicProperties kafkaTopicProperties,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            @Value("${cce.collector.max-payload-size:1048576}") long maxPayloadSize) {
        this.cloudEventValidator = cloudEventValidator;
        this.deduplicationService = deduplicationService;
        this.repository = repository;
        this.enricher = enricher;
        this.payloadValidator = payloadValidator;
        this.eventPublisher = eventPublisher;
        this.rejectionService = rejectionService;
        this.kafkaTopicProperties = kafkaTopicProperties;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.maxPayloadSize = maxPayloadSize;

        this.receivedCounter = Counter.builder("cce.collector.events.received")
                .description("Total events received at POST /v1/events")
                .register(meterRegistry);
        this.acceptedCounter = Counter.builder("cce.collector.events.accepted")
                .description("Events that passed all validation and were published")
                .register(meterRegistry);
        this.duplicateCounter = Counter.builder("cce.collector.events.duplicate")
                .description("Duplicate events detected")
                .register(meterRegistry);
        this.ingestionTimer = Timer.builder("cce.collector.ingestion.duration")
                .description("Full ingestion pipeline duration")
                .register(meterRegistry);
    }

    /**
     * Ingest a single CloudEvents event through the full processing pipeline.
     *
     * @param request the inbound event request (parsed from HTTP POST body)
     * @return response containing the persisted event ID and status
     * @throws CloudEventValidationException if envelope validation fails (→ 400)
     * @throws DuplicateEventException       if a duplicate is detected (→ 200)
     * @throws PayloadValidationException       if payload validation fails (→ 422)
     * @throws KafkaPublishException         if Kafka publish fails (→ 500)
     */
    public EventIngestionResponse ingest(EventIngestionRequest request) {
        Timer.Sample timerSample = Timer.start(meterRegistry);
        receivedCounter.increment();

        // Populate MDC for structured logging
        MDC.put("cloudEventsId", request.getId());
        MDC.put("source", request.getSource());
        MDC.put("subject", request.getSubject());

        log.info("Starting event ingestion: cloudeventsId={}, source={}",
                request.getId(), request.getSource());

        // ── Step 2: CloudEvents envelope validation ────────────────
        cloudEventValidator.validate(request);

        // ── Step 2b: Payload size check ────────────────────────────
        checkPayloadSize(request);

        // ── Step 3: Deduplication check ────────────────────────────
        // A duplicate has a final earlier attempt: accepted, or rejected because the event is wrong.
        if (deduplicationService.isDuplicate(request.getId(), request.getSource())) {
            duplicateCounter.increment();
            InboundEventLog existing = repository
                    .findByCloudeventsIdAndSource(request.getId(), request.getSource())
                    .orElseThrow(() -> new IllegalStateException(
                            "Duplicate detected but original record not found for cloudeventsId="
                                    + request.getId() + ", source=" + request.getSource()));
            throw new DuplicateEventException(existing.getId(), existing.getId());
        }

        // ── Step 4: Persist to inbound_event_log (RECEIVED) ───────────
        // A retry of an event that was never accepted uses its existing row again.
        InboundEventLog inbound = inboundEventForThisAttempt(request);
        inbound = repository.save(inbound);
        log.info("Event persisted: id={}, cloudeventsId={}, status=RECEIVED",
                inbound.getId(), inbound.getCloudeventsId());

        // Steps 5–8 are wrapped in a safety-net try-catch so that any
        // unexpected exception after the initial persist (Step 4) records
        // a rejection rather than leaving the event orphaned as RECEIVED.
        try {
            // ── Step 5: Apply server-side defaults ─────────────────
            enricher.enrich(request, inbound);

            // Update MDC with enriched correlationId
            MDC.put("correlationId", inbound.getCorrelationId());

            // ── Step 6: Payload validation (branched by datacontenttype)
            try {
                payloadValidator.validatePayload(request);
            } catch (PayloadValidationException ex) {
                rejectionService.recordRejection(inbound, ex.getRejectionReason(), ex.getMessage());
                throw ex;
            }

            // ── Step 7: Synchronous Kafka publish ──────────────────
            try {
                eventPublisher.publish(request);
            } catch (KafkaPublishException ex) {
                rejectionService.recordRejection(inbound, RejectionReason.KAFKA_PUBLISH_FAILURE,
                        ex.getMessage());
                throw ex;
            } catch (Exception ex) {
                rejectionService.recordRejection(inbound, RejectionReason.KAFKA_PUBLISH_FAILURE,
                        ex.getMessage());
                throw new KafkaPublishException(
                        kafkaTopicProperties.getInboundEvents(), ex);
            }

            // ── Step 8: Update status = ACCEPTED ───────────────────
            // ACCEPTED means the event is in Kafka. So it is set only after the publish worked.
            inbound.setStatus(InboundStatus.ACCEPTED);
            inbound = repository.save(inbound);
            log.info("Event accepted: id={}, cloudeventsId={}",
                    inbound.getId(), inbound.getCloudeventsId());
        } catch (PayloadValidationException ex) {
            // Already handled above — rethrow as-is
            incrementRejectedCounter(ex.getRejectionReason().name());
            throw ex;
        } catch (KafkaPublishException ex) {
            incrementRejectedCounter(RejectionReason.KAFKA_PUBLISH_FAILURE.name());
            throw ex;
        } catch (Exception ex) {
            // Safety net: unexpected failure after persist — record rejection
            // to prevent orphaned RECEIVED records
            log.error("Unexpected error during post-persist processing for id={}: {}",
                    inbound.getId(), ex.getMessage(), ex);
            safeRecordRejection(inbound, RejectionReason.INTERNAL_ERROR, ex.getMessage());
            incrementRejectedCounter(RejectionReason.INTERNAL_ERROR.name());
            throw ex;
        }

        acceptedCounter.increment();
        timerSample.stop(ingestionTimer);
        return buildResponse(inbound);
    }

    // ────────────────────────────────────────────────────────────────

    /**
     * Increment the {@code cce.collector.events.rejected} counter with a
     * {@code reason} tag distinguishing the rejection cause.
     */
    private void incrementRejectedCounter(String reason) {
        Counter.builder("cce.collector.events.rejected")
                .tag("reason", reason)
                .description("Events rejected by reason")
                .register(meterRegistry)
                .increment();
    }

    /**
     * Attempt to record a rejection, swallowing any exception so the original
     * failure is never masked by a secondary DB error.
     */
    private void safeRecordRejection(InboundEventLog event, RejectionReason reason, String details) {
        try {
            rejectionService.recordRejection(event, reason, details);
        } catch (Exception suppressed) {
            log.error("Failed to record rejection for event id={}: {}",
                    event.getId(), suppressed.getMessage(), suppressed);
        }
    }

    /**
     * Check that the payload does not exceed the configured maximum size.
     *
     * <p>Uses {@code String.length()} as a fast estimate of UTF-8 byte count.
     * For JSON payloads (both {@code application/json} and
     * {@code application/fhir+json}), the content is predominantly ASCII
     * (keys, braces, quotes, digits), so char count closely approximates
     * byte count. This avoids allocating a full {@code byte[]} just for
     * a size gate check.</p>
     *
     * @throws CloudEventValidationException if the payload is too large
     */
    private void checkPayloadSize(EventIngestionRequest request) {
        if (request.getData() != null) {
            String dataStr = request.getData().toString();
            // String.length() returns char count in O(1) with zero allocation.
            // For ASCII-dominant JSON payloads, char count ≈ byte count (conservative lower bound).
            long estimatedBytes = (long) dataStr.length();
            if (estimatedBytes > maxPayloadSize) {
                throw new CloudEventValidationException(List.of(
                        "Payload size " + estimatedBytes + " bytes exceeds maximum allowed "
                                + maxPayloadSize + " bytes"));
            }
        }
    }

    /**
     * Give the {@code inbound_event_log} row for this attempt.
     *
     * <ul>
     *   <li>First attempt: a new row.</li>
     *   <li>Retry of an event whose earlier attempt is not final: the row of that attempt.
     *       The table allows one row per {@code (cloudevents_id, source)}, so the row is
     *       used again. It goes back to RECEIVED and the old rejection is cleared.</li>
     * </ul>
     *
     * <p>Example: the first attempt failed at the Kafka publish and the row is REJECTED.
     * The source sends the event again. The same row is processed again and, when the
     * publish works, becomes ACCEPTED.</p>
     */
    private InboundEventLog inboundEventForThisAttempt(EventIngestionRequest request) {
        return repository.findByCloudeventsIdAndSource(request.getId(), request.getSource())
                .map(earlierAttempt -> resetForRetry(earlierAttempt, request))
                .orElseGet(() -> buildInboundEvent(request));
    }

    /**
     * Put the row of an earlier attempt back to RECEIVED, with the payload of this attempt.
     */
    private InboundEventLog resetForRetry(InboundEventLog earlierAttempt, EventIngestionRequest request) {
        log.info("Retry of an event that was not accepted: id={}, cloudeventsId={}, previousStatus={}, previousReason={}",
                earlierAttempt.getId(), earlierAttempt.getCloudeventsId(),
                earlierAttempt.getStatus(), earlierAttempt.getRejectionReason());
        earlierAttempt.setStatus(InboundStatus.RECEIVED);
        earlierAttempt.setRejectionReason(null);
        earlierAttempt.setErrorDetails(null);
        earlierAttempt.setRawPayload(objectMapper.valueToTree(request).toString());
        return earlierAttempt;
    }

    /**
     * Build an {@link InboundEventLog} entity from the request.
     *
     * <p>Serializes the entire {@link EventIngestionRequest} as-is into {@code rawPayload}
     * with no enrichment — all CloudEvents attributes and the original FHIR data are stored
     * exactly as received. {@code cloudeventsId} and {@code source} are also promoted to
     * dedicated columns to support deduplication queries without parsing the raw JSON.</p>
     */
    private InboundEventLog buildInboundEvent(EventIngestionRequest request) {
        return InboundEventLog.builder()
                .cloudeventsId(request.getId())
                .source(request.getSource())
                .rawPayload(objectMapper.valueToTree(request).toString())
                .build();
    }

    /**
     * Build the success response DTO from the persisted entity.
     */
    private EventIngestionResponse buildResponse(InboundEventLog inbound) {
        return EventIngestionResponse.builder()
                .eventId(inbound.getId().toString())
                .cloudEventsId(inbound.getCloudeventsId())
                .status("accepted")
                .correlationId(inbound.getCorrelationId())
                .receivedAt(inbound.getReceivedAt())
                .build();
    }
}
