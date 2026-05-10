package com.outboxsagalab.fx.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Background poller that ships unpublished outbox rows to Kafka.
 *
 * Lives in its own class — separate from {@code FxService} — so the
 * outbox seam is visible: domain code never publishes to Kafka, it only writes
 * to the outbox table. This poller is the single producer.
 *
 * Runs in its own transaction (see {@code @Transactional} on {@link #flush()})
 * — explicitly NOT joined with the consumer's domain transaction.
 *
 * If a publish succeeds but the row update fails (process crash between the
 * two), the same event will be re-published. Consumers de-duplicate via the
 * {@code processed_events} table — see docs/architecture.md §4.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_SIZE = 50;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000L)
    @Transactional
    public void flush() {
        List<OutboxEntry> batch = outboxRepository.findUnpublished(PageRequest.of(0, BATCH_SIZE));
        if (batch.isEmpty()) {
            return;
        }
        log.debug("Outbox flush: {} unpublished rows", batch.size());
        for (OutboxEntry entry : batch) {
            try {
                // Block until ack — keeps the poller simple. For lab volume this is fine.
                kafkaTemplate.send(entry.getTopic(), entry.getAggregateId(), entry.getPayload()).get();
                entry.markPublished();
                log.info("Published event_id={} type={} topic={} aggregate_id={}",
                        entry.getEventId(), entry.getEventType(), entry.getTopic(), entry.getAggregateId());
            } catch (Exception e) {
                // Leave published_at = null so we retry next tick. Log and stop the
                // batch so we don't busy-loop on a broken broker.
                log.error("Failed to publish outbox event_id={} — will retry next tick: {}",
                        entry.getEventId(), e.toString());
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
