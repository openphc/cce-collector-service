package org.openphc.cce.collector.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openphc.cce.collector.config.DeduplicationProperties;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventLogRepository;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DeduplicationService}.
 *
 * <p>Uses a mocked {@link InboundEventLogRepository} to verify deduplication
 * logic without requiring a database. Covers the four scenarios defined
 * in Sub-Task C7:</p>
 * <ol>
 *   <li>First submission → not duplicate (repository returns false)</li>
 *   <li>Second identical submission → duplicate (repository returns true)</li>
 *   <li>Same ID, different source → not duplicate</li>
 *   <li>Lookback window respected in query parameter</li>
 * </ol>
 */
@ExtendWith(MockitoExtension.class)
class DeduplicationServiceTest {

    @Mock
    private InboundEventLogRepository repository;

    private DeduplicationProperties properties;
    private DeduplicationService service;

    private static final String CLOUD_EVENTS_ID = "evt-001";
    private static final String SOURCE = "rhie-mediator";

    @BeforeEach
    void setUp() {
        properties = new DeduplicationProperties();
        properties.setLookbackDays(30);
        service = new DeduplicationService(repository, properties);
    }

    // ════════════════════════════════════════════════════════════════
    // First submission — not duplicate
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("First submission (not duplicate)")
    class FirstSubmission {

        @Test
        @DisplayName("returns false when repository finds no match")
        void notDuplicate() {
            when(repository.existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(SOURCE), any(OffsetDateTime.class),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames())))
                    .thenReturn(false);

            boolean result = service.isDuplicate(CLOUD_EVENTS_ID, SOURCE);

            assertThat(result).isFalse();
        }

        @Test
        @DisplayName("queries repository with correct id and source")
        void queriesWithCorrectParams() {
            when(repository.existsFinalAttempt(
                    any(), any(), any(), any(), any(), any())).thenReturn(false);

            service.isDuplicate(CLOUD_EVENTS_ID, SOURCE);

            verify(repository).existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(SOURCE), any(OffsetDateTime.class),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames()));
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Second identical submission — duplicate
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Second identical submission (duplicate)")
    class SecondIdenticalSubmission {

        @Test
        @DisplayName("returns true when repository finds a match")
        void isDuplicate() {
            when(repository.existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(SOURCE), any(OffsetDateTime.class),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames())))
                    .thenReturn(true);

            boolean result = service.isDuplicate(CLOUD_EVENTS_ID, SOURCE);

            assertThat(result).isTrue();
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Same ID, different source — not duplicate
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Same ID, different source (not duplicate)")
    class SameIdDifferentSource {

        @Test
        @DisplayName("returns false when source differs")
        void notDuplicateWithDifferentSource() {
            String differentSource = "another-mediator";

            when(repository.existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(differentSource), any(OffsetDateTime.class),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames())))
                    .thenReturn(false);

            boolean result = service.isDuplicate(CLOUD_EVENTS_ID, differentSource);

            assertThat(result).isFalse();
            verify(repository).existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(differentSource), any(OffsetDateTime.class),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames()));
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Lookback window
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Lookback window")
    class LookbackWindow {

        @Test
        @DisplayName("default lookback is 30 days")
        void defaultLookback30Days() {
            when(repository.existsFinalAttempt(
                    any(), any(), any(), any(), any(), any())).thenReturn(false);

            service.isDuplicate(CLOUD_EVENTS_ID, SOURCE);

            ArgumentCaptor<OffsetDateTime> captor = ArgumentCaptor.forClass(OffsetDateTime.class);
            verify(repository).existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(SOURCE), captor.capture(),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames()));

            OffsetDateTime lookbackDate = captor.getValue();
            OffsetDateTime expected = OffsetDateTime.now().minusDays(30);

            // Allow 2-second tolerance for test execution time
            assertThat(lookbackDate).isCloseTo(expected, within(2, ChronoUnit.SECONDS));
        }

        @Test
        @DisplayName("custom lookback window is respected")
        void customLookbackWindow() {
            properties.setLookbackDays(7);

            when(repository.existsFinalAttempt(
                    any(), any(), any(), any(), any(), any())).thenReturn(false);

            service.isDuplicate(CLOUD_EVENTS_ID, SOURCE);

            ArgumentCaptor<OffsetDateTime> captor = ArgumentCaptor.forClass(OffsetDateTime.class);
            verify(repository).existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(SOURCE), captor.capture(),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames()));

            OffsetDateTime lookbackDate = captor.getValue();
            OffsetDateTime expected = OffsetDateTime.now().minusDays(7);

            assertThat(lookbackDate).isCloseTo(expected, within(2, ChronoUnit.SECONDS));
        }

        @Test
        @DisplayName("lookback window of 1 day queries only recent events")
        void oneDayLookback() {
            properties.setLookbackDays(1);

            when(repository.existsFinalAttempt(
                    any(), any(), any(), any(), any(), any())).thenReturn(false);

            service.isDuplicate(CLOUD_EVENTS_ID, SOURCE);

            ArgumentCaptor<OffsetDateTime> captor = ArgumentCaptor.forClass(OffsetDateTime.class);
            verify(repository).existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(SOURCE), captor.capture(),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames()));

            OffsetDateTime lookbackDate = captor.getValue();
            OffsetDateTime expected = OffsetDateTime.now().minusDays(1);

            assertThat(lookbackDate).isCloseTo(expected, within(2, ChronoUnit.SECONDS));
        }

        @Test
        @DisplayName("lookback of 0 days queries from now (effectively no lookback)")
        void zeroLookback() {
            properties.setLookbackDays(0);

            when(repository.existsFinalAttempt(
                    any(), any(), any(), any(), any(), any())).thenReturn(false);

            service.isDuplicate(CLOUD_EVENTS_ID, SOURCE);

            ArgumentCaptor<OffsetDateTime> captor = ArgumentCaptor.forClass(OffsetDateTime.class);
            verify(repository).existsFinalAttempt(
                    eq(CLOUD_EVENTS_ID), eq(SOURCE), captor.capture(),
                    eq(InboundStatus.ACCEPTED), eq(InboundStatus.REJECTED), eq(RejectionReason.retryableNames()));

            OffsetDateTime lookbackDate = captor.getValue();
            OffsetDateTime expected = OffsetDateTime.now();

            assertThat(lookbackDate).isCloseTo(expected, within(2, ChronoUnit.SECONDS));
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Configuration binding
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Configuration")
    class Configuration {

        @Test
        @DisplayName("properties default lookback-days is 30")
        void defaultProperties() {
            DeduplicationProperties defaults = new DeduplicationProperties();
            assertThat(defaults.getLookbackDays()).isEqualTo(30);
        }

        @Test
        @DisplayName("properties lookback-days is settable")
        void settableProperties() {
            DeduplicationProperties props = new DeduplicationProperties();
            props.setLookbackDays(60);
            assertThat(props.getLookbackDays()).isEqualTo(60);
        }
    }
}
