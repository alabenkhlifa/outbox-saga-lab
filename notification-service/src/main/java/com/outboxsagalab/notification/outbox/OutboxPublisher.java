package com.outboxsagalab.notification.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Background poller that publishes unsent {@link OutboxEntry} rows to Kafka.
 *
 * <p>This is the second half of the textbook outbox pattern — the first half
 * (atomic write of domain row + outbox row) lives in
 * {@code NotificationService}. Keeping the poller in a separate class makes the
 * seam visible: we never publish to Kafka from business code.
 *
 * <p>Each poll runs in its own transaction. If publish succeeds but the
 * {@code published_at} update fails, we'll re-publish on the next tick — that
 * is exactly why every consumer is idempotent.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    /** How many rows to drain per tick. Small to keep ordering obvious. */
    private static final int BATCH_SIZE = 50;

    /** Bounded wait for the Kafka send so we never hang the scheduler thread forever. */
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPending() {
        List<OutboxEntry> batch = outboxRepository.findUnpublished(PageRequest.of(0, BATCH_SIZE));
        if (batch.isEmpty()) {
            return;
        }

        log.debug("outbox poll picked up batch size={}", batch.size());

        for (OutboxEntry entry : batch) {
            try {
                kafkaTemplate
                        .send(entry.getTopic(), entry.getAggregateId(), entry.getPayload())
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                entry.markPublished(Instant.now());
                outboxRepository.save(entry);

                log.info("outbox published event_id={} event_type={} topic={} aggregate_id={}",
                        entry.getEventId(), entry.getEventType(), entry.getTopic(), entry.getAggregateId());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("outbox publish interrupted event_id={} — will retry", entry.getEventId(), ie);
                return;
            } catch (ExecutionException | TimeoutException ex) {
                // Stop the batch on first failure so ordering is preserved on the next tick.
                log.warn("outbox publish failed event_id={} event_type={} — will retry on next tick",
                        entry.getEventId(), entry.getEventType(), ex);
                return;
            }
        }
    }
}
