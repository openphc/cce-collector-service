package org.openphc.cce.collector.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Publishing and topic-creation settings for this service, bound to {@code cce.kafka}.
 *
 * <p>The topic <em>name</em> is not here. It comes from cce-common-util's
 * {@link org.openphc.cce.common.kafka.KafkaTopicProperties}, under {@code cce.kafka.topics}, so that
 * the service producing to {@code cce.events.inbound} and the service consuming from it read the same
 * property rather than each carrying its own spelling of the same topic.
 *
 * <p>What remains is genuinely local: how long a synchronous send may block, and the partition and
 * retention settings used to create the topic on startup. No other service creates this topic.
 *
 * <pre>
 * cce:
 *   kafka:
 *     topics:
 *       inbound-events: cce.events.inbound   # shared — KafkaTopicProperties
 *     publish-timeout-seconds: 10
 *     topic-config:
 *       partitions: 25
 *       replication-factor: 1
 *       retention-ms: 604800000
 *       cleanup-policy: delete
 *       min-insync-replicas: 1
 * </pre>
 */
@Component
@ConfigurationProperties(prefix = "cce.kafka")
@Getter
@Setter
public class KafkaPublishProperties {

    /**
     * Maximum time (in seconds) to block waiting for a Kafka broker
     * acknowledgement on a synchronous send. Default: 30 seconds.
     */
    private int publishTimeoutSeconds = 30;

    /**
     * Topic creation configuration. Used by {@code KafkaTopicConfig}
     * to programmatically create the inbound topic on application startup.
     */
    private TopicConfig topicConfig = new TopicConfig();

    @Getter
    @Setter
    public static class TopicConfig {

        /** Number of partitions for the inbound topic. Default: 25. */
        private int partitions = 25;

        /** Replication factor for the inbound topic. Default: 1 (single-broker local dev). */
        private short replicationFactor = 1;

        /** Retention period in milliseconds. Default: 604800000 (7 days). */
        private long retentionMs = 604_800_000L;

        /** Cleanup policy. Default: {@code delete}. */
        private String cleanupPolicy = "delete";

        /** Minimum number of in-sync replicas. Default: 1. */
        private int minInsyncReplicas = 1;
    }
}
