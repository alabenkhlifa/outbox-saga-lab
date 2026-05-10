package com.outboxsagalab.transfer.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The outbox poller — runs in its OWN transaction, independent of the
 * domain saga's transaction. This is the second of the two transactions
 * shown in docs/architecture.md section 4.
 *
 * Algorithm:
 *   1. SELECT oldest N unpublished rows
 *   2. For each row: build the envelope JSON, publish to Kafka,
 *      block on the future so a failure throws.
 *   3. Mark each row published_at = now().
 *
 * If anything throws inside the @Transactional method, the whole batch
 * rolls back and we retry on the next tick. That is fine: every consumer
 * dedupes via processed_events, so re-publishing is safe.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    /** Per-tick batch size. Small for the lab — easy to read in logs. */
    private static final int BATCH_SIZE = 50;

    /** Hard cap on each Kafka send so a stuck broker doesn't pin the poller. */
    private static final long PUBLISH_TIMEOUT_SECONDS = 5L;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Polls every second. Separate @Transactional => separate DB transaction
     * from the domain/saga writes. This is the visible seam between
     * "write outbox" and "publish outbox".
     */
    @Scheduled(fixedDelay = 1000L)
    @Transactional
    public void publishPending() {
        List<OutboxEntry> batch =
                outboxRepository.findUnpublished(PageRequest.of(0, BATCH_SIZE));

        if (batch.isEmpty()) {
            return;
        }

        log.info("Outbox poll: {} unpublished row(s)", batch.size());

        for (OutboxEntry entry : batch) {
            String json = renderEnvelope(entry);
            sendBlocking(entry, json);
            entry.markPublished();
            log.info("Outbox -> Kafka topic={} event_type={} event_id={} saga_id={}",
                    entry.getTopic(), entry.getEventType(), entry.getEventId(), entry.getAggregateId());
        }
    }

    /**
     * Build the on-the-wire envelope from an outbox row. The payload is
     * stored as JSONB; the rest of the envelope fields come from the row's
     * columns plus a fresh occurred_at stamp.
     */
    private String renderEnvelope(OutboxEntry entry) {
        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("event_id", entry.getEventId().toString());
        envelope.put("event_type", entry.getEventType());
        envelope.put("saga_id", entry.getAggregateId());
        envelope.put("occurred_at", Instant.now().toString());
        envelope.set("payload", entry.getPayload());

        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            // Payload was stored as JsonNode — serialization should never fail.
            throw new IllegalStateException(
                    "Failed to serialize outbox envelope for event_id=" + entry.getEventId(), e);
        }
    }

    /**
     * Block on the send so that any failure (timeout, broker down,
     * serialization error) throws and rolls back the whole tick.
     */
    private void sendBlocking(OutboxEntry entry, String json) {
        try {
            SendResult<String, String> result = kafkaTemplate
                    .send(entry.getTopic(), entry.getAggregateId(), json)
                    .get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.debug("Kafka ack: topic={} partition={} offset={}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted publishing event_id=" + entry.getEventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException(
                    "Failed publishing event_id=" + entry.getEventId(), e);
        }
    }
}
