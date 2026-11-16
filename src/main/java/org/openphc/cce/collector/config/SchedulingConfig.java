package org.openphc.cce.collector.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Turns on scheduled jobs. The only job is
 * {@link org.openphc.cce.collector.service.RejectedEventRepublisher}.
 * Set {@code cce.collector.republish.enabled=false} to turn it off.
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "cce.collector.republish", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class SchedulingConfig {
}
