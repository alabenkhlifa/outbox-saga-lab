package com.outboxsagalab.payment.outbox;

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
 * Background drainer for the outbox table.
 *
 * <p>This is a deliberately separate class from {@code PaymentService} so
 * the seam is visible: TX 1 (consumer + domain + outbox insert) and
 * TX 2 (this poller) commit independently. If the publish succeeds but the
 * {@code published_at} update fails, the message will be republished on
 * the next tick — which is fine because every consumer downstream checks
 * {@code processed_events} before doing any work.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    /** Max rows to drain per tick. Keeps each TX bounded. */
    private static final int BATCH_SIZE = 50;

    /** How long to wait for the broker to ack a single send. */
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void drainOutbox() {
        List<OutboxEntry> batch = outboxRepository.findUnpublished(PageRequest.of(0, BATCH_SIZE));
        if (batch.isEmpty()) {
            return;
        }

        log.debug("outbox poller: draining {} row(s)", batch.size());

        for (OutboxEntry entry : batch) {
            try {
                // Block on get() so any send failure throws and rolls the TX back.
                kafkaTemplate
                        .send(entry.getTopic(), entry.getAggregateId(), entry.getPayload())
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                entry.markPublished(Instant.now());
                log.info("published outbox event saga_id={} event_type={} event_id={} topic={}",
                        entry.getAggregateId(), entry.getEventType(), entry.getEventId(), entry.getTopic());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while publishing outbox row id=" + entry.getId(), e);
            } catch (ExecutionException | TimeoutException e) {
                // Re-throw so Spring rolls back the transaction. The row stays
                // unpublished and we'll try again on the next tick.
                throw new IllegalStateException("failed to publish outbox row id=" + entry.getId(), e);
            }
        }
    }
}
