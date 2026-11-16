package org.openphc.cce.collector.domain.model.enums;

import java.util.Arrays;
import java.util.List;

/**
 * Reasons an inbound event can be rejected.
 *
 * <p>Stored as VARCHAR in the database (not as an enum column) to allow
 * flexibility for future reason codes without schema changes.</p>
 *
 * <p><b>NOTE:</b> No FailureStage enum exists — the failure stage is fully
 * derivable from the rejection reason (e.g. {@code KAFKA_PUBLISH_FAILURE}
 * implies the Kafka publish stage; all others imply validation or processing).</p>
 *
 * <p>There are two kinds of reason:</p>
 * <ul>
 *   <li><b>The event is wrong.</b> Example: {@code INVALID_FHIR}. Sending the same event
 *       again gives the same result. The rejection is final.</li>
 *   <li><b>The Collector failed.</b> {@code KAFKA_PUBLISH_FAILURE} and {@code INTERNAL_ERROR}.
 *       The event itself may be fine. Only these are tried again. See {@link #isRetryable()}.</li>
 * </ul>
 */
public enum RejectionReason {
    INVALID_ENVELOPE,
    INVALID_FHIR,
    INVALID_JSON,
    UNSUPPORTED_CONTENT_TYPE,
    DUPLICATE,
    MISSING_SUBJECT,
    PAYLOAD_TOO_LARGE,
    DESERIALIZATION_ERROR,
    KAFKA_PUBLISH_FAILURE,
    INTERNAL_ERROR;

    /**
     * Whether an event rejected for this reason is tried again.
     *
     * <p>True only when the Collector failed, not the event:
     * {@code KAFKA_PUBLISH_FAILURE} and {@code INTERNAL_ERROR}.</p>
     */
    public boolean isRetryable() {
        return this == KAFKA_PUBLISH_FAILURE || this == INTERNAL_ERROR;
    }

    /**
     * The names of the reasons that are tried again, as stored in
     * {@code inbound_event_log.rejection_reason}.
     */
    public static List<String> retryableNames() {
        return Arrays.stream(values())
                .filter(RejectionReason::isRetryable)
                .map(RejectionReason::name)
                .toList();
    }
}
