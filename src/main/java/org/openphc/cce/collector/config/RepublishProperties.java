package org.openphc.cce.collector.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Settings for publishing again the events whose Kafka publish failed.
 * Bound to {@code cce.collector.republish}.
 *
 * <pre>
 * cce:
 *   collector:
 *     republish:
 *       enabled: true
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

    /** Turn the scheduled republish on or off. */
    private boolean enabled = true;

    /** Wait time between two runs, in milliseconds. */
    private long intervalMs = 60_000;

    /** Most events published again in one run. */
    private int batchSize = 500;
}
