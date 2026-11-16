package org.openphc.cce.collector.domain.repository;

import org.openphc.cce.collector.domain.model.InboundEventLog;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Collection;
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
     * {@link #existsFinalAttempt}.</p>
     *
     * @param cloudeventsId the CloudEvents id
     * @param source        the event source
     * @return the matching event, if any
     */
    Optional<InboundEventLog> findByCloudeventsIdAndSource(String cloudeventsId, String source);

    /**
     * Check if an earlier attempt of this event is final, within the lookback window.
     *
     * <p>This is the <strong>primary dedup decision</strong> used during event
     * ingestion (Step 3 of the processing pipeline). An earlier attempt is final when:</p>
     * <ul>
     *   <li>it was accepted (the event is in Kafka); or</li>
     *   <li>it was rejected because the event is wrong (for example INVALID_FHIR).</li>
     * </ul>
     *
     * <p>An earlier attempt is <strong>not</strong> final when it was rejected for one of
     * {@code retryableReasons} (the Collector failed), or when it is still RECEIVED.
     * Returns a lightweight boolean — no entity hydration — keeping the hot path fast.</p>
     *
     * @param cloudeventsId    the CloudEvents id
     * @param source           the event source
     * @param since            the start of the lookback window (e.g. {@code now() − 30 days})
     * @param accepted         {@link InboundStatus#ACCEPTED}
     * @param rejected         {@link InboundStatus#REJECTED}
     * @param retryableReasons the rejection reasons that are tried again
     * @return true if a final earlier attempt exists after the given date
     */
    @Query("""
            select case when count(e) > 0 then true else false end
            from InboundEventLog e
            where e.cloudeventsId = :cloudeventsId
              and e.source = :source
              and e.receivedAt > :since
              and (e.status = :accepted
                   or (e.status = :rejected and e.rejectionReason not in :retryableReasons))
            """)
    boolean existsFinalAttempt(@Param("cloudeventsId") String cloudeventsId,
                               @Param("source") String source,
                               @Param("since") OffsetDateTime since,
                               @Param("accepted") InboundStatus accepted,
                               @Param("rejected") InboundStatus rejected,
                               @Param("retryableReasons") Collection<String> retryableReasons);

    /**
     * Find the oldest events with the given status and one of the given rejection reasons.
     * Used to try again the events that were rejected because the Collector failed.
     *
     * @param status           the status to filter by (REJECTED)
     * @param rejectionReasons the rejection reasons to filter by (the retryable ones)
     * @param pageable         how many rows to read
     * @return the matching events, oldest first
     */
    List<InboundEventLog> findByStatusAndRejectionReasonInOrderByReceivedAtAsc(
            InboundStatus status, Collection<String> rejectionReasons, Pageable pageable);

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
