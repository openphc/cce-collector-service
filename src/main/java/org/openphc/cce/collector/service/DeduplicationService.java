package org.openphc.cce.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.config.DeduplicationProperties;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventLogRepository;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

/**
 * PostgreSQL-based deduplication service with a configurable lookback window.
 *
 * <p>Checks whether an earlier attempt of the same event (same CloudEvents {@code id}
 * and {@code source}) is final, within the lookback window (default 30 days).</p>
 *
 * <ul>
 *   <li><b>Accepted:</b> final. The event is in Kafka. The new submission is a duplicate.</li>
 *   <li><b>Rejected because the event is wrong</b> (for example INVALID_FHIR): final.
 *       The same event gives the same result. The new submission is a duplicate.</li>
 *   <li><b>Rejected because the Collector failed</b> (KAFKA_PUBLISH_FAILURE, INTERNAL_ERROR):
 *       not final. The new submission is processed.</li>
 *   <li><b>Still RECEIVED</b> (the Collector stopped in the middle): not final.
 *       The new submission is processed.</li>
 * </ul>
 *
 * <p>Example: the first attempt failed at the Kafka publish and is REJECTED with
 * KAFKA_PUBLISH_FAILURE. The retry is processed, not answered "duplicate".</p>
 *
 * <p>The Collector follows the <strong>idempotent POST</strong> pattern:
 * duplicate submissions receive HTTP 200 (not 409 Conflict) with a
 * {@code "duplicate"} status.</p>
 *
 * @see DeduplicationProperties
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeduplicationService {

    private final InboundEventLogRepository repository;
    private final DeduplicationProperties properties;

    /**
     * Check whether an earlier attempt of the event with the given CloudEvents id
     * and source is final, within the configurable lookback window.
     *
     * <p>Uses {@link InboundEventLogRepository#existsFinalAttempt}
     * which returns a lightweight boolean — no entity hydration — keeping
     * the hot path fast.</p>
     *
     * @param cloudEventsId the CloudEvents {@code id} attribute
     * @param source        the CloudEvents {@code source} attribute
     * @return {@code true} if a final earlier attempt exists within the lookback
     *         window; {@code false} otherwise
     */
    public boolean isDuplicate(String cloudEventsId, String source) {
        OffsetDateTime lookbackDate = OffsetDateTime.now()
                .minusDays(properties.getLookbackDays());

        boolean duplicate = repository.existsFinalAttempt(
                cloudEventsId, source, lookbackDate,
                InboundStatus.ACCEPTED, InboundStatus.REJECTED, RejectionReason.retryableNames());

        if (duplicate) {
            log.info("Duplicate event detected: cloudEventsId='{}', source='{}'",
                    cloudEventsId, source);
        } else {
            log.debug("No duplicate found for cloudEventsId='{}', source='{}'",
                    cloudEventsId, source);
        }

        return duplicate;
    }
}
