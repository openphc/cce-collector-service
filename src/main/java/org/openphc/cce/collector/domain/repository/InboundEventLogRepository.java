package org.openphc.cce.collector.domain.repository;

import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data JPA repository for {@link InboundEventLog}.
 *
 * <p>All rejected-event queries operate on this single table —
 * no separate dead_letter_event table exists.</p>
 */
@Repository
public interface InboundEventLogRepository extends JpaRepository<InboundEventLog, UUID> {

    /**
     * Find an event by its CloudEvents id and source (exact match).
     *
     * <p>Used to fetch the row itself:</p>
     * <ul>
     *   <li>to return the existing {@code eventId} in the idempotent 200 duplicate response;</li>
     *   <li>to use the row of an earlier attempt again, when the source retries an event
     *       that was never accepted.</li>
     * </ul>
     *
     * <p><strong>Not</strong> the dedup decision method — see
     * {@link #existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter}.</p>
     *
     * @param cloudeventsId the CloudEvents id
     * @param source        the event source
     * @return the matching event, if any
     */
    Optional<InboundEventLog> findByCloudeventsIdAndSource(String cloudeventsId, String source);

    /**
     * Check if an event with the given status exists within the lookback window.
     *
     * <p>This is the <strong>primary dedup decision</strong> used during event
     * ingestion (Step 3 of the processing pipeline), called with status ACCEPTED.
     * A row that is RECEIVED or REJECTED never reached Kafka, so it does not make
     * the retry a duplicate. Returns a lightweight boolean — no entity hydration —
     * keeping the hot path fast.</p>
     *
     * @param cloudeventsId the CloudEvents id
     * @param source        the event source
     * @param status        the status the earlier event must have (ACCEPTED)
     * @param since         the start of the lookback window (e.g. {@code now() − 30 days})
     * @return true if a matching event with that status exists after the given date
     */
    boolean existsByCloudeventsIdAndSourceAndStatusAndReceivedAtAfter(
            String cloudeventsId, String source, InboundStatus status, OffsetDateTime since);

    /**
     * Find the oldest events with the given status and rejection reason.
     * Used to publish again the events whose Kafka publish failed.
     *
     * @param status          the status to filter by (REJECTED)
     * @param rejectionReason the rejection reason to filter by (KAFKA_PUBLISH_FAILURE)
     * @param pageable        how many rows to read
     * @return the matching events, oldest first
     */
    List<InboundEventLog> findByStatusAndRejectionReasonOrderByReceivedAtAsc(
            InboundStatus status, String rejectionReason, Pageable pageable);

    /**
     * Find events by status, ordered by most recent first.
     * Used by the rejected-event management endpoints.
     *
     * @param status   the status to filter by (typically REJECTED)
     * @param pageable pagination parameters
     * @return paginated list of events with the given status
     */
    Page<InboundEventLog> findByStatusOrderByReceivedAtDesc(
            InboundStatus status, Pageable pageable);

    /**
     * Count events by status.
     * Used for the rejected gauge metric.
     *
     * @param status the status to count
     * @return count of matching events
     */
    long countByStatus(InboundStatus status);
}
