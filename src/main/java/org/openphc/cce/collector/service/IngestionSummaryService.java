package org.openphc.cce.collector.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.domain.model.InboundEvent;
import org.openphc.cce.collector.domain.repository.EventVolumeDailyRepository;
import org.openphc.cce.collector.domain.repository.IngestionSummaryDailyRepository;
import org.openphc.cce.collector.domain.repository.PipelineLossDailyRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

/**
 * Maintains pre-computed summary tables for the Insights Service.
 *
 * <p>Called by {@link EventIngestionService} at key pipeline decision points
 * to incrementally update daily aggregation tables. Each upsert is atomic
 * (INSERT ... ON CONFLICT DO UPDATE) and runs in its own transaction.</p>
 *
 * <p>Failures in summary updates are logged but never propagate to the caller —
 * the core ingestion pipeline must not fail due to insights pre-computation.</p>
 */
@Slf4j
@Service
public class IngestionSummaryService {

    private final IngestionSummaryDailyRepository ingestionSummaryRepo;
    private final EventVolumeDailyRepository eventVolumeRepo;
    private final PipelineLossDailyRepository pipelineLossRepo;

    public IngestionSummaryService(
            IngestionSummaryDailyRepository ingestionSummaryRepo,
            EventVolumeDailyRepository eventVolumeRepo,
            PipelineLossDailyRepository pipelineLossRepo) {
        this.ingestionSummaryRepo = ingestionSummaryRepo;
        this.eventVolumeRepo = eventVolumeRepo;
        this.pipelineLossRepo = pipelineLossRepo;
    }

    /**
     * Record an event in the ingestion summary (all events — received, accepted, rejected, duplicate).
     *
     * @param event        the persisted inbound event
     * @param resourceType the FHIR resource type (may be null for non-FHIR payloads)
     */
    public void recordIngestion(InboundEvent event, String resourceType) {
        try {
            ingestionSummaryRepo.upsertEventCount(
                    LocalDate.now(),
                    event.getSource(),
                    event.getFacilityId(),
                    event.getStatus().name(),
                    event.getRejectionReason(),
                    resourceType);
        } catch (Exception ex) {
            log.warn("Failed to update ingestion_summary_daily for event id={}: {}",
                    event.getId(), ex.getMessage());
        }
    }

    /**
     * Record an accepted event in the volume and pipeline loss tables.
     *
     * <p>Called only for events that reach ACCEPTED status and are published to Kafka.</p>
     *
     * @param event        the accepted inbound event
     * @param resourceType the FHIR resource type (may be null for non-FHIR payloads)
     */
    public void recordAccepted(InboundEvent event, String resourceType) {
        String resolvedResourceType = resourceType != null ? resourceType : "unknown";

        try {
            eventVolumeRepo.upsertEventCount(
                    LocalDate.now(),
                    event.getSource(),
                    event.getFacilityId(),
                    resolvedResourceType);
        } catch (Exception ex) {
            log.warn("Failed to update event_volume_daily for event id={}: {}",
                    event.getId(), ex.getMessage());
        }

        try {
            pipelineLossRepo.upsertAcceptedCount(
                    LocalDate.now(),
                    event.getSource(),
                    event.getFacilityId(),
                    resolvedResourceType);
        } catch (Exception ex) {
            log.warn("Failed to update pipeline_loss_daily for event id={}: {}",
                    event.getId(), ex.getMessage());
        }
    }

    /**
     * Extract the FHIR resource type from a raw JSON data payload.
     *
     * @param data the event data node (may be null)
     * @return the resourceType string, or null if not present
     */
    public String extractResourceType(JsonNode data) {
        if (data == null || !data.has("resourceType")) {
            return null;
        }
        JsonNode resourceTypeNode = data.get("resourceType");
        if (resourceTypeNode.isNull() || resourceTypeNode.asText().isBlank()) {
            return null;
        }
        return resourceTypeNode.asText();
    }
}
