package org.openphc.cce.collector.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Settings for trying again the stored events that were rejected because the
 * Collector failed (KAFKA_PUBLISH_FAILURE, INTERNAL_ERROR).
 * Bound to {@code cce.collector.republish}. The job is off by default.
 *
 * <pre>
 * cce:
 *   collector:
 *     republish:
 *       enabled: false
 *       interval-ms: 60000
 *       batch-size: 500
 * </pre>
 *
 * @see org.openphc.cce.collector.service.RejectedEventRepublisher
 */
@Component
@ConfigurationProperties(prefix = "cce.collector.republish")
@Getter
@Setter
public class RepublishProperties {

    /** Turn the scheduled job on or off. Off by default. */
    private boolean enabled = false;

    /** Wait time between two runs, in milliseconds. */
    private long intervalMs = 60_000;

    /** Most events tried again in one run. */
    private int batchSize = 500;
}
