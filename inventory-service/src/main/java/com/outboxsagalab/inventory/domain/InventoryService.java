package com.outboxsagalab.inventory.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.outboxsagalab.inventory.idempotency.ProcessedEvent;
import com.outboxsagalab.inventory.idempotency.ProcessedEventRepository;
import com.outboxsagalab.inventory.messaging.Topics;
import com.outboxsagalab.inventory.outbox.OutboxEntry;
import com.outboxsagalab.inventory.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Core domain operations for the inventory participant.
 *
 * Each public method is a single {@code @Transactional} unit that:
 * <ol>
 *   <li>checks {@code processed_events} for the inbound {@code event_id} and skips if already seen,</li>
 *   <li>performs the domain change (decrement / increment stock + insert reservation rows),</li>
 *   <li>writes one row into the {@code outbox} table with the reply event,</li>
 *   <li>inserts the {@code processed_events} marker.</li>
 * </ol>
 *
 * The outbox row is committed atomically with the domain change — the
 * {@code OutboxPublisher} forwards it to Kafka in a separate transaction.
 *
 * <p><b>Concurrency:</b> {@code reserveForOrder} relies on an atomic conditional
 * UPDATE in {@link StockRepository#tryDecrement(String, int)} — a row's
 * {@code available_qty} is decremented only if it is &gt;= the requested qty.
 * Two parallel ReserveStock commands for the same SKU cannot both win: at most
 * one UPDATE will see enough stock; the other returns 0 rows affected and we
 * emit StockReservationFailed.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final StockRepository stockRepository;
    private final ReservationRepository reservationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public InventoryService(StockRepository stockRepository,
                            ReservationRepository reservationRepository,
                            ProcessedEventRepository processedEventRepository,
                            OutboxRepository outboxRepository,
                            ObjectMapper objectMapper) {
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Handle a {@code ReserveStock} command.
     *
     * Concurrency note: the per-SKU decrement uses a conditional native UPDATE
     * (see {@link StockRepository#tryDecrement(String, int)}). It is safe under
     * concurrent reservation attempts for the same SKU — exactly one wins.
     */
    @Transactional
    public void reserveForOrder(UUID inboundEventId,
                                UUID sagaId,
                                UUID orderId,
                                List<ReservationItem> items) {
        // 1) Idempotency check — first thing inside the transaction.
        if (processedEventRepository.existsById(inboundEventId)) {
            log.info("Skipping ReserveStock event_id={} (already processed)", inboundEventId);
            return;
        }

        log.info("Reserving stock for order_id={} saga_id={} items={}", orderId, sagaId, items);

        boolean allReserved = true;
        String failureReason = null;

        // 2) Try to decrement each SKU atomically. We commit per-line attempts
        //    to the reservations table either way (FAILED or RESERVED) so the
        //    audit trail is complete.
        for (ReservationItem item : items) {
            int affected = stockRepository.tryDecrement(item.sku(), item.qty());
            if (affected == 1) {
                Reservation r = new Reservation(UUID.randomUUID(), orderId, item.sku(), item.qty(), ReservationStatus.RESERVED);
                reservationRepository.save(r);
                log.info("  reserved sku={} qty={} order_id={}", item.sku(), item.qty(), orderId);
            } else {
                Reservation r = new Reservation(UUID.randomUUID(), orderId, item.sku(), item.qty(), ReservationStatus.FAILED);
                reservationRepository.save(r);
                allReserved = false;
                failureReason = "insufficient_stock_or_unknown_sku:" + item.sku();
                log.warn("  reservation failed sku={} qty={} order_id={} reason={}",
                        item.sku(), item.qty(), orderId, failureReason);
                break;
            }
        }

        // 3) If we partially succeeded then encountered a failure, roll back
        //    the increments we already did so the saga can be cleanly compensated.
        if (!allReserved) {
            for (Reservation r : reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED)) {
                stockRepository.increment(r.getSku(), r.getQty());
                r.markReleased();
                log.info("  rolling back partial reservation sku={} qty={}", r.getSku(), r.getQty());
            }
        }

        // 4) Write the outbox row carrying the reply event.
        if (allReserved) {
            enqueueOutbox("StockReserved", sagaId, orderId, env -> {
                ObjectNode payload = env.putObject("payload");
                payload.put("order_id", orderId.toString());
                ArrayNode arr = payload.putArray("items");
                for (ReservationItem it : items) {
                    ObjectNode line = arr.addObject();
                    line.put("sku", it.sku());
                    line.put("qty", it.qty());
                }
            });
        } else {
            final String reason = failureReason;
            enqueueOutbox("StockReservationFailed", sagaId, orderId, env -> {
                ObjectNode payload = env.putObject("payload");
                payload.put("order_id", orderId.toString());
                payload.put("reason", reason);
            });
        }

        // 5) Mark the inbound event as processed — same transaction.
        processedEventRepository.save(new ProcessedEvent(inboundEventId, "ReserveStock"));
    }

    /**
     * Handle a {@code ReleaseStock} command (compensation).
     *
     * Increments stock back for every RESERVED row of the given order and
     * flips the reservation rows to RELEASED. Emits StockReleased on the
     * inventory-events topic.
     */
    @Transactional
    public void releaseForOrder(UUID inboundEventId, UUID sagaId, UUID orderId) {
        // 1) Idempotency check — first thing inside the transaction.
        if (processedEventRepository.existsById(inboundEventId)) {
            log.info("Skipping ReleaseStock event_id={} (already processed)", inboundEventId);
            return;
        }

        log.info("Releasing stock for order_id={} saga_id={}", orderId, sagaId);

        // 2) Increment stock for each currently-RESERVED line and flip status.
        List<Reservation> reserved = reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED);
        for (Reservation r : reserved) {
            stockRepository.increment(r.getSku(), r.getQty());
            r.markReleased();
            log.info("  released sku={} qty={} order_id={}", r.getSku(), r.getQty(), orderId);
        }

        // 3) Outbox the StockReleased reply.
        enqueueOutbox("StockReleased", sagaId, orderId, env -> {
            ObjectNode payload = env.putObject("payload");
            payload.put("order_id", orderId.toString());
            payload.put("released_lines", reserved.size());
        });

        // 4) Mark inbound processed — same transaction.
        processedEventRepository.save(new ProcessedEvent(inboundEventId, "ReleaseStock"));
    }

    /**
     * Build the standard event envelope (see docs/architecture.md §5) and
     * insert it as a single outbox row. The {@link com.outboxsagalab.inventory.outbox.OutboxPublisher}
     * picks it up asynchronously.
     */
    private void enqueueOutbox(String eventType, UUID sagaId, UUID orderId, EnvelopeBuilder builder) {
        UUID eventId = UUID.randomUUID();
        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("event_id", eventId.toString());
        envelope.put("event_type", eventType);
        envelope.put("saga_id", sagaId.toString());
        envelope.put("occurred_at", Instant.now().toString());
        builder.fill(envelope);

        String payload;
        try {
            payload = objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            // Should be impossible — we only put primitives + nested ObjectNodes.
            throw new IllegalStateException("Failed to serialize outbox envelope", e);
        }

        OutboxEntry entry = new OutboxEntry(
                eventId,
                orderId.toString(),
                Topics.INVENTORY_EVENTS,
                eventType,
                payload
        );
        outboxRepository.save(entry);
        log.info("Enqueued outbox event_id={} type={} order_id={}", eventId, eventType, orderId);
    }

    @FunctionalInterface
    private interface EnvelopeBuilder {
        void fill(ObjectNode envelope);
    }
}
