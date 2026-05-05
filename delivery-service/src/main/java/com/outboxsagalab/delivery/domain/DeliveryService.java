package com.outboxsagalab.delivery.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.delivery.idempotency.ProcessedEvent;
import com.outboxsagalab.delivery.idempotency.ProcessedEventRepository;
import com.outboxsagalab.delivery.messaging.EventEnvelope;
import com.outboxsagalab.delivery.messaging.Topics;
import com.outboxsagalab.delivery.outbox.OutboxEntry;
import com.outboxsagalab.delivery.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Core domain logic for the delivery-service.
 *
 * <p>Each public method is a single transaction that:
 * <ol>
 *   <li>checks {@link ProcessedEventRepository} for the inbound {@code event_id}
 *       and short-circuits if seen (textbook idempotent consumer);</li>
 *   <li>performs the domain change (insert / update {@link Delivery});</li>
 *   <li>writes a row into {@link com.outboxsagalab.delivery.outbox.OutboxEntry}
 *       so the outbox poller can publish it (textbook outbox).</li>
 * </ol>
 *
 * <p>Publishing to Kafka happens out-of-band in
 * {@link com.outboxsagalab.delivery.outbox.OutboxPublisher} — never from here.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    /** Stub: roughly 95% of ScheduleDelivery commands succeed. */
    private static final int SUCCESS_PERCENT = 95;

    /** Stub: pretend the courier shows up half an hour from now. */
    private static final Duration DEFAULT_LEAD_TIME = Duration.ofMinutes(30);

    private final DeliveryRepository deliveryRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public DeliveryService(DeliveryRepository deliveryRepository,
                           ProcessedEventRepository processedEventRepository,
                           OutboxRepository outboxRepository,
                           ObjectMapper objectMapper) {
        this.deliveryRepository = deliveryRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Handle a {@code ScheduleDelivery} command.
     *
     * <p>Stub success/fail decision (~95% success). On success we emit
     * {@code DeliveryScheduled}; on failure we emit {@code DeliveryFailed}
     * which causes the orchestrator to compensate the saga.
     */
    @Transactional
    public void scheduleForOrder(UUID inboundEventId,
                                 String inboundEventType,
                                 UUID sagaId,
                                 UUID orderId,
                                 String address) {
        // 1) Idempotency check — MUST be first thing in the transaction.
        if (alreadyProcessed(inboundEventId, inboundEventType, sagaId)) {
            return;
        }

        log.info("delivery handling ScheduleDelivery saga_id={} order_id={} event_id={}",
                sagaId, orderId, inboundEventId);

        // 2) Domain change — pick success/fail and persist a deliveries row.
        boolean success = ThreadLocalRandom.current().nextInt(100) < SUCCESS_PERCENT;
        log.info("delivery decision saga_id={} decision={}", sagaId, success ? "SCHEDULED" : "FAILED");

        UUID deliveryId = UUID.randomUUID();
        Instant scheduledAt = success ? Instant.now().plus(DEFAULT_LEAD_TIME) : null;
        DeliveryStatus status = success ? DeliveryStatus.SCHEDULED : DeliveryStatus.FAILED;

        Delivery delivery = new Delivery(deliveryId, orderId, address, scheduledAt, status);
        deliveryRepository.save(delivery);

        // 3) Mark inbound as processed (still inside the same transaction).
        recordProcessed(inboundEventId, inboundEventType);

        // 4) Outbox the reply event.
        if (success) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("delivery_id", deliveryId.toString());
            payload.put("order_id", orderId.toString());
            payload.put("scheduled_at", scheduledAt.toString());
            enqueueOutbox(sagaId, "DeliveryScheduled", payload);
        } else {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("delivery_id", deliveryId.toString());
            payload.put("order_id", orderId.toString());
            payload.put("reason", "scheduling-stub-rolled-fail");
            enqueueOutbox(sagaId, "DeliveryFailed", payload);
        }
    }

    /**
     * Handle a {@code CancelDelivery} command (compensation).
     *
     * <p>Always emits {@code DeliveryCancelled} once committed, even if the
     * delivery row is not found — the orchestrator must always get its reply.
     */
    @Transactional
    public void cancelForOrder(UUID inboundEventId,
                               String inboundEventType,
                               UUID sagaId,
                               UUID orderId) {
        // 1) Idempotency check — MUST be first thing in the transaction.
        if (alreadyProcessed(inboundEventId, inboundEventType, sagaId)) {
            return;
        }

        log.info("delivery handling CancelDelivery saga_id={} order_id={} event_id={}",
                sagaId, orderId, inboundEventId);

        // 2) Domain change — flip the most recent delivery row to CANCELLED if present.
        Optional<Delivery> existing = deliveryRepository.findFirstByOrderIdOrderByCreatedAtDesc(orderId);
        UUID deliveryId;
        if (existing.isPresent()) {
            Delivery delivery = existing.get();
            delivery.setStatus(DeliveryStatus.CANCELLED);
            delivery.setScheduledAt(null);
            deliveryRepository.save(delivery);
            deliveryId = delivery.getId();
            log.info("delivery cancelled existing row saga_id={} delivery_id={}", sagaId, deliveryId);
        } else {
            // Nothing to cancel — log and still emit the reply so the saga can advance.
            deliveryId = null;
            log.warn("delivery cancel requested but no delivery found saga_id={} order_id={}", sagaId, orderId);
        }

        // 3) Mark inbound as processed.
        recordProcessed(inboundEventId, inboundEventType);

        // 4) Outbox the reply event.
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("order_id", orderId.toString());
        payload.put("delivery_id", deliveryId == null ? null : deliveryId.toString());
        enqueueOutbox(sagaId, "DeliveryCancelled", payload);
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private boolean alreadyProcessed(UUID inboundEventId, String inboundEventType, UUID sagaId) {
        if (processedEventRepository.existsById(inboundEventId)) {
            log.info("delivery skipping duplicate event_id={} event_type={} saga_id={}",
                    inboundEventId, inboundEventType, sagaId);
            return true;
        }
        return false;
    }

    private void recordProcessed(UUID inboundEventId, String inboundEventType) {
        processedEventRepository.save(new ProcessedEvent(inboundEventId, inboundEventType));
    }

    private void enqueueOutbox(UUID sagaId, String eventType, Map<String, Object> payload) {
        UUID outboundEventId = UUID.randomUUID();
        EventEnvelope envelope = new EventEnvelope(
                outboundEventId,
                eventType,
                sagaId,
                Instant.now(),
                payload
        );

        String json;
        try {
            json = objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            // Failing to serialise our own envelope is a programming error — fail the TX so we don't
            // commit the domain change without an outbox row.
            throw new IllegalStateException("Could not serialise outbound envelope for event " + eventType, e);
        }

        OutboxEntry entry = new OutboxEntry(
                outboundEventId,
                sagaId.toString(),
                Topics.DELIVERY_EVENTS,
                eventType,
                json
        );
        outboxRepository.save(entry);

        log.info("delivery outboxed event_id={} event_type={} saga_id={}",
                outboundEventId, eventType, sagaId);
    }
}
