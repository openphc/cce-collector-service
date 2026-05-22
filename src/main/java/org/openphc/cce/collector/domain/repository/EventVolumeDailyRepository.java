package org.openphc.cce.collector.domain.repository;

import org.openphc.cce.collector.domain.model.EventVolumeDaily;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Repository for {@link EventVolumeDaily} pre-computed table.
 *
 * <p>Uses native upsert for atomic increment on each accepted event.</p>
 */
@Repository
public interface EventVolumeDailyRepository extends JpaRepository<EventVolumeDaily, UUID> {

    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO event_volume_daily
                (id, summary_date, source, facility_id, resource_type, event_count, updated_at)
            VALUES
                (gen_random_uuid(), :summaryDate, :source, :facilityId, :resourceType, 1, now())
            ON CONFLICT (summary_date, source, facility_id, resource_type)
            DO UPDATE SET
                event_count = event_volume_daily.event_count + 1,
                updated_at = now()
            """, nativeQuery = true)
    void upsertEventCount(
            @Param("summaryDate") LocalDate summaryDate,
            @Param("source") String source,
            @Param("facilityId") String facilityId,
            @Param("resourceType") String resourceType);
}
