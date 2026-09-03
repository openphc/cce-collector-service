package org.openphc.cce.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.hl7.fhir.r4.model.ResourceType;
import org.openphc.cce.common.fhir.ClinicalEventTimeExtractor;
import org.openphc.cce.common.fhir.ResourceTypeDetector;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Enriches <strong>both</strong> the {@link InboundEventLog} entity and the
 * {@link EventIngestionRequest} DTO with server-side defaults for optional
 * fields.
 *
 * <p>This is <strong>not</strong> normalization — source-provided values are
 * never modified. Only <em>absent</em> optional fields are filled:</p>
 *
 * <ol>
 *   <li>{@code correlationId / correlationid} — generated as {@code "corr-" + UUID} if absent</li>
 *   <li>{@code time} — filled with the server's {@code receivedAt} timestamp if absent (on request DTO only, for Kafka publishing)</li>
 *   <li>{@code eventTime} — derived clinical occurrence time set on the entity (persistence only). Extracted
 *       from the FHIR payload via {@link ClinicalEventTimeExtractor}; falls back to the CloudEvents envelope
 *       {@code time}, then to {@code receivedAt}, when no clinical field is present.</li>
 * </ol>
 *
 * <p>The request DTO is also updated so that when it is published to Kafka
 * it carries the enriched values. The entity remains the authoritative
 * persistence record.</p>
 *
 * <p>The event {@code type} field is <strong>never</strong> modified —
 * it passes through unchanged from the source system.</p>
 */
@Slf4j
@Service
public class EventDefaultsEnricher {

    private static final String CORRELATION_ID_PREFIX = "corr-";

    private final ClinicalEventTimeExtractor clinicalEventTimeExtractor;
    private final ResourceTypeDetector resourceTypeDetector;

    public EventDefaultsEnricher(ClinicalEventTimeExtractor clinicalEventTimeExtractor,
                                 ResourceTypeDetector resourceTypeDetector) {
        this.clinicalEventTimeExtractor = clinicalEventTimeExtractor;
        this.resourceTypeDetector = resourceTypeDetector;
    }

    /**
     * Enrich both the entity and the request with server-side defaults.
     *
     * <p>Reads optional fields from the {@code request}. When a field is
     * absent, generates a default and sets it on <strong>both</strong> the
     * entity (for persistence) and the request (for Kafka publishing).</p>
     *
     * @param request the inbound event request (mutated with defaults)
     * @param inbound the JPA entity to enrich for persistence
     */
    public void enrich(EventIngestionRequest request, InboundEventLog inbound) {
        enrichCorrelationId(request, inbound);
        enrichTime(request, inbound);
        enrichEventTime(request, inbound);
    }

    /**
     * Generate a correlation ID if absent: {@code "corr-" + UUID}.
     * Sets the value on both entity and request.
     */
    private void enrichCorrelationId(EventIngestionRequest request, InboundEventLog inbound) {
        if (isBlank(request.getCorrelationid())) {
            String generated = CORRELATION_ID_PREFIX + UUID.randomUUID();
            inbound.setCorrelationId(generated);
            request.setCorrelationid(generated);
            log.debug("Generated correlationId: {}", generated);
        } else {
            inbound.setCorrelationId(request.getCorrelationid());
        }
    }

    /**
     * Fill {@code time} on the request DTO with the server's {@code receivedAt} if absent.
     * Only enriches the request (for Kafka publishing) — no entity field to set.
     */
    private void enrichTime(EventIngestionRequest request, InboundEventLog inbound) {
        if (isBlank(request.getTime())) {
            OffsetDateTime receivedAt = inbound.getReceivedAt();
            request.setTime(receivedAt.toString());
            log.debug("Filled time from server receivedAt: {}", receivedAt);
        }
    }

    /**
     * Derive the clinical occurrence time and set it on the entity ({@code event_time} column).
     *
     * <p>Precedence: (1) clinical time extracted from the FHIR payload via
     * {@link ClinicalEventTimeExtractor}; (2) the CloudEvents envelope {@code time}
     * (already filled by {@link #enrichTime} when absent); (3) the server's {@code receivedAt}.
     * Only the entity is set — the envelope {@code time} on the request is left untouched.</p>
     */
    private void enrichEventTime(EventIngestionRequest eventIngestionRequest, InboundEventLog inboundEventLog) {
        OffsetDateTime clinicalEventTime = extractClinicalEventTime(eventIngestionRequest);
        if (clinicalEventTime != null) {
            inboundEventLog.setEventTime(clinicalEventTime);
            log.debug("Set eventTime from clinical payload: {}", clinicalEventTime);
            return;
        }
        OffsetDateTime envelopeTime = parseEnvelopeTime(eventIngestionRequest.getTime());
        OffsetDateTime fallbackEventTime = envelopeTime != null ? envelopeTime : inboundEventLog.getReceivedAt();
        inboundEventLog.setEventTime(fallbackEventTime);
        log.debug("Set eventTime from fallback (envelope time / receivedAt): {}", fallbackEventTime);
    }

    /**
     * Best-effort clinical event time extraction. Never throws — a malformed payload
     * simply yields {@code null} so the caller falls back to the envelope time.
     */
    private OffsetDateTime extractClinicalEventTime(EventIngestionRequest eventIngestionRequest) {
        try {
            ResourceType fhirResourceType =
                    resourceTypeDetector.detect(eventIngestionRequest.getData());
            return clinicalEventTimeExtractor.extract(fhirResourceType, eventIngestionRequest.getData());
        } catch (Exception ex) {
            log.warn("Failed to extract clinical event time — falling back: {}", ex.getMessage());
            return null;
        }
    }

    /**
     * Parse the CloudEvents envelope {@code time} (ISO-8601) to an {@link OffsetDateTime},
     * returning {@code null} if absent or unparseable.
     */
    private OffsetDateTime parseEnvelopeTime(String envelopeTime) {
        if (isBlank(envelopeTime)) {
            return null;
        }
        try {
            return OffsetDateTime.parse(envelopeTime);
        } catch (Exception ex) {
            log.warn("Unparseable envelope time '{}' — falling back to receivedAt", envelopeTime);
            return null;
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
