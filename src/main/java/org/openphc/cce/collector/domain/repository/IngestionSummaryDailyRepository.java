package org.openphc.cce.collector.domain.repository;

import org.openphc.cce.collector.domain.model.IngestionSummaryDaily;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Repository for {@link IngestionSummaryDaily} pre-computed table.
 *
 * <p>Uses native upsert (INSERT ... ON CONFLICT DO UPDATE) for atomic
 * increment operations on each event ingestion.</p>
 */
@Repository
public interface IngestionSummaryDailyRepository extends JpaRepository<IngestionSummaryDaily, UUID> {

    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO ingestion_summary_daily
                (id, summary_date, source, facility_id, status, rejection_reason, resource_type, event_count, distinct_patients, updated_at)
            VALUES
                (gen_random_uuid(), :summaryDate, :source, :facilityId, :status, :rejectionReason, :resourceType, 1, 1, now())
            ON CONFLICT (summary_date, source, facility_id, status, rejection_reason, resource_type)
            DO UPDATE SET
                event_count = ingestion_summary_daily.event_count + 1,
                updated_at = now()
            """, nativeQuery = true)
    void upsertEventCount(
            @Param("summaryDate") LocalDate summaryDate,
            @Param("source") String source,
            @Param("facilityId") String facilityId,
            @Param("status") String status,
            @Param("rejectionReason") String rejectionReason,
            @Param("resourceType") String resourceType);
}
