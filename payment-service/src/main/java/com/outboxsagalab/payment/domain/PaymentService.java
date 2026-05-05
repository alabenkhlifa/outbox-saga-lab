package com.outboxsagalab.payment.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.outboxsagalab.payment.idempotency.ProcessedEvent;
import com.outboxsagalab.payment.idempotency.ProcessedEventRepository;
import com.outboxsagalab.payment.messaging.Topics;
import com.outboxsagalab.payment.outbox.OutboxEntry;
import com.outboxsagalab.payment.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Domain service that handles {@code RequestPayment} and {@code RefundPayment}
 * commands.
 *
 * <p>Each public method is a single {@code @Transactional} unit that performs
 * the four-step textbook recipe for an idempotent saga participant:
 *
 * <ol>
 *   <li>Check the {@code processed_events} table for the inbound event_id; if
 *       present, return immediately (a redelivery — already handled).</li>
 *   <li>Perform the domain work (insert/update a {@link Payment} row).</li>
 *   <li>Insert a row into {@code processed_events} so future redeliveries are
 *       detected.</li>
 *   <li>Insert a row into the outbox describing the reply event. The outbox
 *       row is drained to Kafka by {@link com.outboxsagalab.payment.outbox.OutboxPublisher}
 *       in a separate transaction.</li>
 * </ol>
 *
 * <p>All four steps share one DB transaction. Either everything commits or
 * everything rolls back; Kafka will redeliver the command if the TX rolls
 * back, and the idempotency check catches duplicates.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    /** Approval rate of the stub charge logic, in percent. */
    private static final int APPROVAL_RATE_PERCENT = 85;

    private final PaymentRepository payments;
    private final OutboxRepository outbox;
    private final ProcessedEventRepository processedEvents;
    private final ObjectMapper objectMapper;

    public PaymentService(PaymentRepository payments,
                          OutboxRepository outbox,
                          ProcessedEventRepository processedEvents,
                          ObjectMapper objectMapper) {
        this.payments = payments;
        this.outbox = outbox;
        this.processedEvents = processedEvents;
        this.objectMapper = objectMapper;
    }

    /**
     * Handles a {@code RequestPayment} command. Decides approve/decline
     * with a stub random charge and emits {@code PaymentApproved} or
     * {@code PaymentDeclined} via the outbox.
     */
    @Transactional
    public void chargeForOrder(UUID inboundEventId,
                               UUID sagaId,
                               UUID orderId,
                               BigDecimal amount,
                               String currency) {
        // 1. Idempotency check — first thing inside the TX.
        if (processedEvents.existsById(inboundEventId)) {
            log.info("skipping already-processed RequestPayment event_id={} saga_id={}",
                    inboundEventId, sagaId);
            return;
        }

        // 2. Domain work: stub charge decision, persist the payment row.
        boolean approved = decideApproval();
        PaymentStatus status = approved ? PaymentStatus.APPROVED : PaymentStatus.DECLINED;
        log.info("payment decision saga_id={} order_id={} amount={} {} decision={}",
                sagaId, orderId, amount, currency, status);

        Payment payment = new Payment(UUID.randomUUID(), orderId, amount, currency, status);
        payments.save(payment);

        // 3. Mark the inbound event as processed.
        processedEvents.save(new ProcessedEvent(inboundEventId, "RequestPayment"));

        // 4. Emit the reply via the outbox (same TX as steps 2 + 3).
        String replyType = approved ? "PaymentApproved" : "PaymentDeclined";
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("payment_id", payment.getId().toString());
        payload.put("order_id", orderId.toString());
        payload.put("amount", amount);
        payload.put("currency", currency);
        payload.put("status", status.name());

        enqueueOutboxEvent(sagaId, replyType, payload);
    }

    /**
     * Handles a {@code RefundPayment} command. Records a REFUNDED row and
     * emits {@code PaymentRefunded} via the outbox.
     */
    @Transactional
    public void refund(UUID inboundEventId, UUID sagaId, UUID orderId) {
        // 1. Idempotency check.
        if (processedEvents.existsById(inboundEventId)) {
            log.info("skipping already-processed RefundPayment event_id={} saga_id={}",
                    inboundEventId, sagaId);
            return;
        }

        // 2. Domain work: write a REFUNDED row mirroring the most recent
        //    APPROVED row for this order. If no approved row exists (which
        //    shouldn't happen on the happy path), we still write a refund
        //    row with zero amount so the audit trail is consistent.
        BigDecimal amount = payments.findByOrderId(orderId).stream()
                .filter(p -> p.getStatus() == PaymentStatus.APPROVED)
                .map(Payment::getAmount)
                .findFirst()
                .orElse(BigDecimal.ZERO);
        String currency = payments.findByOrderId(orderId).stream()
                .filter(p -> p.getStatus() == PaymentStatus.APPROVED)
                .map(Payment::getCurrency)
                .findFirst()
                .orElse("EUR");

        Payment refund = new Payment(UUID.randomUUID(), orderId, amount, currency, PaymentStatus.REFUNDED);
        payments.save(refund);
        log.info("refund recorded saga_id={} order_id={} amount={} {}", sagaId, orderId, amount, currency);

        // 3. Mark inbound event processed.
        processedEvents.save(new ProcessedEvent(inboundEventId, "RefundPayment"));

        // 4. Emit reply.
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("payment_id", refund.getId().toString());
        payload.put("order_id", orderId.toString());
        payload.put("amount", amount);
        payload.put("currency", currency);
        payload.put("status", PaymentStatus.REFUNDED.name());

        enqueueOutboxEvent(sagaId, "PaymentRefunded", payload);
    }

    /**
     * Builds the canonical envelope JSON and writes the outbox row. The row
     * is committed with the rest of the transaction; the background poller
     * picks it up and pushes it to Kafka.
     */
    private void enqueueOutboxEvent(UUID sagaId, String eventType, ObjectNode innerPayload) {
        UUID outboundEventId = UUID.randomUUID();
        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("event_id", outboundEventId.toString());
        envelope.put("event_type", eventType);
        envelope.put("saga_id", sagaId.toString());
        envelope.put("occurred_at", Instant.now().toString());
        envelope.set("payload", innerPayload);

        String json;
        try {
            json = objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            // ObjectNode is always serializable — this is unreachable in practice.
            throw new IllegalStateException("failed to serialize outbox envelope", e);
        }

        OutboxEntry entry = new OutboxEntry(
                outboundEventId,
                sagaId.toString(),
                Topics.PAYMENT_EVENTS,
                eventType,
                json
        );
        outbox.save(entry);
        log.info("enqueued outbox event saga_id={} event_type={} event_id={}",
                sagaId, eventType, outboundEventId);
    }

    private boolean decideApproval() {
        return ThreadLocalRandom.current().nextInt(100) < APPROVAL_RATE_PERCENT;
    }
}
