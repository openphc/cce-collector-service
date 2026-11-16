package org.openphc.cce.collector.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Turns on scheduled jobs. The only job is
 * {@link org.openphc.cce.collector.service.RejectedEventRepublisher}.
 * It is off by default. Set {@code cce.collector.republish.enabled=true} to turn it on.
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "cce.collector.republish", name = "enabled", havingValue = "true")
public class SchedulingConfig {
}
