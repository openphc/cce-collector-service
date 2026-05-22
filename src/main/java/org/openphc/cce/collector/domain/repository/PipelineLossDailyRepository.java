package org.openphc.cce.collector.domain.repository;

import org.openphc.cce.collector.domain.model.PipelineLossDaily;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Repository for {@link PipelineLossDaily} pre-computed table.
 *
 * <p>The Collector Service increments {@code accepted_count} on event acceptance.
 * The Compliance Service increments {@code matched_count} when events are matched.</p>
 */
@Repository
public interface PipelineLossDailyRepository extends JpaRepository<PipelineLossDaily, UUID> {

    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO pipeline_loss_daily
                (id, summary_date, source, facility_id, resource_type, accepted_count, matched_count, updated_at)
            VALUES
                (gen_random_uuid(), :summaryDate, :source, :facilityId, :resourceType, 1, 0, now())
            ON CONFLICT (summary_date, source, facility_id, resource_type)
            DO UPDATE SET
                accepted_count = pipeline_loss_daily.accepted_count + 1,
                updated_at = now()
            """, nativeQuery = true)
    void upsertAcceptedCount(
            @Param("summaryDate") LocalDate summaryDate,
            @Param("source") String source,
            @Param("facilityId") String facilityId,
            @Param("resourceType") String resourceType);
}
