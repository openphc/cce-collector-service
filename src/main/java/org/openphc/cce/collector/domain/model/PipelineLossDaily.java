package org.openphc.cce.collector.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Pre-computed daily pipeline loss tracking for the Insights Service.
 *
 * <p>Tracks the delta between events accepted by the Collector and
 * events matched by the Compliance Service. The {@code accepted_count}
 * is updated by the Collector; {@code matched_count} is updated by the
 * Compliance Service (cross-service write to shared DB).</p>
 */
@Entity
@Table(name = "pipeline_loss_daily")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PipelineLossDaily {

    @Id
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "summary_date", nullable = false)
    private LocalDate summaryDate;

    @Column(name = "source", nullable = false, length = 100)
    private String source;

    @Column(name = "facility_id", length = 100)
    private String facilityId;

    @Column(name = "resource_type", length = 100)
    private String resourceType;

    @Column(name = "accepted_count", nullable = false)
    @Builder.Default
    private long acceptedCount = 0;

    @Column(name = "matched_count", nullable = false)
    @Builder.Default
    private long matchedCount = 0;

    @Column(name = "unmatched_count", insertable = false, updatable = false)
    private Long unmatchedCount;

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private OffsetDateTime updatedAt = OffsetDateTime.now();

    @PrePersist
    void generateId() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
    }
}
