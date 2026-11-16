package org.openphc.cce.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.config.DeduplicationProperties;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.repository.InboundEventLogRepository;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

/**
 * PostgreSQL-based deduplication service with a configurable lookback window.
 *
 * <p>Checks whether an event with the same CloudEvents {@code id} and
 * {@code source} was already <strong>accepted</strong> within the lookback window
 * (default 30 days). Accepted means the event was published to Kafka.</p>
 *
 * <p>An earlier attempt that is still RECEIVED or REJECTED is not a duplicate.
 * That event never reached Kafka, so the retry must be processed.
 * Example: the first attempt failed at the Kafka publish and is REJECTED.
 * The retry is processed, not answered "duplicate".</p>
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
     * Check whether an event with the given CloudEvents id and source
     * was already accepted within the configurable lookback window.
     *
     * <p>Uses {@link InboundEventLogRepository#existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter}
     * which returns a lightweight boolean — no entity hydration — keeping
     * the hot path fast.</p>
     *
     * @param cloudEventsId the CloudEvents {@code id} attribute
     * @param source        the CloudEvents {@code source} attribute
     * @return {@code true} if a matching accepted event exists within the lookback
     *         window; {@code false} otherwise
     */
    public boolean isDuplicate(String cloudEventsId, String source) {
        OffsetDateTime lookbackDate = OffsetDateTime.now()
                .minusDays(properties.getLookbackDays());

        boolean duplicate = repository.existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter(
                cloudEventsId, source, InboundStatus.ACCEPTED, lookbackDate);

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
