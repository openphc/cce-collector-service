package org.openphc.cce.collector.config;

import org.openphc.cce.common.config.FhirConfig;
import org.openphc.cce.common.fhir.ClinicalEventTimeExtractor;
import org.openphc.cce.common.fhir.ResourceTypeDetector;
import org.openphc.cce.common.kafka.KafkaTopicProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The beans this service takes from cce-common-util.
 *
 * <p>{@link FhirConfig} supplies the {@code FhirContext}: expensive to build, thread-safe once built,
 * and necessarily the same R4 context every CCE service parses against.
 *
 * <p>{@link ClinicalEventTimeExtractor} derives the clinical occurrence time stamped on
 * {@code inbound_event_log.event_time}. It has to be the same reading the Matcher Service later judges
 * an SLA against, from the same payload — the two services held byte-equivalent copies of the candidate
 * table by the time this was written, having already had to reconcile them once when they disagreed
 * about whether an {@code Encounter} occurred at {@code period.start} or {@code period.end}.
 *
 * <p>{@link ResourceTypeDetector} reads the {@code resourceType} discriminator off an inbound
 * payload — used here to reject a non-FHIR resource at validation and to pick the clinical-time field
 * above. The Matcher Service reads it from the same payload to look up triggers, and the two services
 * had drifted onto different lookups ({@code fromCode} here, {@code valueOf} there) and different
 * tolerance of a null node, so what counted as an unrecognized type depended on which service asked.
 *
 * <p>{@link KafkaTopicProperties} names the topic this service produces to and the Matcher Service
 * consumes from, so the two cannot drift onto different spellings of one topic.
 *
 * <p>Imported by name rather than by widening {@code scanBasePackages} to {@code org.openphc.cce}, as
 * the other services do. They consume the library's entities and repositories; this service owns one
 * table and has no business reaching for the runtime plane's — and scanning the whole library would
 * also replace the {@code ObjectMapper} Spring Boot configures for this service's HTTP layer and add a
 * second {@code GlobalExceptionHandler} beside this service's own.
 *
 * <p>The imports live here, in a scanned {@code @Configuration}, rather than on
 * {@code CollectorServiceApplication}: an {@code @Import} on the application class is honoured even by
 * slice tests such as {@code @DataJpaTest}, which would then have to satisfy a {@code MeterRegistry}
 * they have no reason to configure.
 */
@Configuration
@Import({FhirConfig.class, ClinicalEventTimeExtractor.class, ResourceTypeDetector.class,
        KafkaTopicProperties.class})
public class CommonUtilConfig {
}
