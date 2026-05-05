package com.outboxsagalab.order.saga;

import com.outboxsagalab.order.domain.Order;
import com.outboxsagalab.order.domain.OrderRepository;
import com.outboxsagalab.order.domain.OrderStatus;
import com.outboxsagalab.order.idempotency.ProcessedEvent;
import com.outboxsagalab.order.idempotency.ProcessedEventRepository;
import com.outboxsagalab.order.messaging.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * The saga state machine.
 *
 * One method handles every inbound event. The flow inside that method is:
 *
 *   1. IDEMPOTENCY CHECK (FIRST). If we've already processed this event_id
 *      we return early — Kafka redelivery is fine.
 *   2. Load the order by saga_id.
 *   3. Decide the next state + next outbox row based on (current state,
 *      event_type).
 *   4. Insert ProcessedEvent for the inbound event_id.
 *
 * The whole thing is one @Transactional. Either the domain change AND the
 * outbox row AND the processed_events row all commit, or nothing does.
 *
 * State machine summary (happy path + compensations):
 *
 *   PENDING            --(create)--> outbox: RequestPayment, state PAYMENT_REQUESTED
 *   PAYMENT_REQUESTED  --PaymentApproved--> outbox: ReserveStock, state STOCK_REQUESTED... wait, see below
 *
 *   Reading docs/architecture.md the state names map differently:
 *     after PaymentApproved -> PAID, then immediately emit ReserveStock and move to STOCK_REQUESTED.
 *     after StockReserved   -> RESERVED, then immediately emit ScheduleDelivery and move to DELIVERY_REQUESTED.
 *     after DeliveryScheduled -> COMPLETED.
 *
 *   Compensations (reverse order):
 *     PaymentDeclined (from PAYMENT_REQUESTED)        -> FAILED. Nothing to compensate.
 *     StockReservationFailed (from STOCK_REQUESTED)   -> COMPENSATING + outbox: RefundPayment.
 *     PaymentRefunded after stock fail                -> FAILED.
 *     DeliveryFailed (from DELIVERY_REQUESTED)        -> COMPENSATING + outbox: ReleaseStock.
 *     StockReleased after delivery fail               -> outbox: RefundPayment (state stays COMPENSATING).
 *     PaymentRefunded after delivery+stock fail       -> FAILED.
 */
@Component
public class OrderSaga {

    private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final CommandEmitter commands;

    public OrderSaga(OrderRepository orderRepository,
                     ProcessedEventRepository processedEventRepository,
                     CommandEmitter commands) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.commands = commands;
    }

    /**
     * Single entry point for inbound events.
     *
     * Idempotency is the FIRST thing checked. Domain change + outbox row +
     * processed_events insert all happen in this one transaction.
     */
    @Transactional
    public void handle(EventEnvelope envelope) {
        UUID eventId = envelope.eventId();
        String type = envelope.eventType();
        UUID sagaId = envelope.sagaId();

        // 1. Idempotency check — FIRST.
        if (processedEventRepository.existsById(eventId)) {
            log.info("Skipping duplicate event event_id={} event_type={} saga_id={}",
                    eventId, type, sagaId);
            return;
        }

        log.info("Saga consumed event_type={} event_id={} saga_id={}", type, eventId, sagaId);

        Order order = orderRepository.findById(sagaId)
                .orElseThrow(() -> new IllegalStateException(
                        "Unknown saga_id=" + sagaId + " for event_id=" + eventId));

        // 2. Decide next state and (maybe) next command.
        switch (type) {
            case EventTypes.PAYMENT_APPROVED          -> onPaymentApproved(order);
            case EventTypes.PAYMENT_DECLINED          -> onPaymentDeclined(order);
            case EventTypes.STOCK_RESERVED            -> onStockReserved(order);
            case EventTypes.STOCK_RESERVATION_FAILED  -> onStockReservationFailed(order);
            case EventTypes.DELIVERY_SCHEDULED        -> onDeliveryScheduled(order);
            case EventTypes.DELIVERY_FAILED           -> onDeliveryFailed(order);
            case EventTypes.STOCK_RELEASED            -> onStockReleased(order);
            case EventTypes.PAYMENT_REFUNDED          -> onPaymentRefunded(order);
            default -> log.warn("Saga ignoring unknown event_type={} event_id={} saga_id={}",
                    type, eventId, sagaId);
        }

        // 3. Mark the inbound event processed (after domain work succeeded).
        processedEventRepository.save(new ProcessedEvent(eventId, type));
    }

    // --------------------------------------------------------------------- //
    // Happy-path handlers                                                   //
    // --------------------------------------------------------------------- //

    private void onPaymentApproved(Order order) {
        require(order, OrderStatus.PAYMENT_REQUESTED, EventTypes.PAYMENT_APPROVED);
        transition(order, OrderStatus.PAID);
        commands.reserveStock(order);
        transition(order, OrderStatus.STOCK_REQUESTED);
    }

    private void onStockReserved(Order order) {
        require(order, OrderStatus.STOCK_REQUESTED, EventTypes.STOCK_RESERVED);
        transition(order, OrderStatus.RESERVED);
        commands.scheduleDelivery(order);
        transition(order, OrderStatus.DELIVERY_REQUESTED);
    }

    private void onDeliveryScheduled(Order order) {
        require(order, OrderStatus.DELIVERY_REQUESTED, EventTypes.DELIVERY_SCHEDULED);
        transition(order, OrderStatus.COMPLETED);
    }

    // --------------------------------------------------------------------- //
    // Compensation handlers                                                 //
    // --------------------------------------------------------------------- //

    private void onPaymentDeclined(Order order) {
        require(order, OrderStatus.PAYMENT_REQUESTED, EventTypes.PAYMENT_DECLINED);
        // Nothing to compensate — payment never charged.
        transition(order, OrderStatus.FAILED);
    }

    private void onStockReservationFailed(Order order) {
        require(order, OrderStatus.STOCK_REQUESTED, EventTypes.STOCK_RESERVATION_FAILED);
        // Reverse-order compensation: refund the payment that already went through.
        transition(order, OrderStatus.COMPENSATING);
        commands.refundPayment(order);
    }

    private void onDeliveryFailed(Order order) {
        require(order, OrderStatus.DELIVERY_REQUESTED, EventTypes.DELIVERY_FAILED);
        // Reverse-order compensation: release stock first, refund happens after StockReleased.
        transition(order, OrderStatus.COMPENSATING);
        commands.releaseStock(order);
    }

    private void onStockReleased(Order order) {
        // Only valid mid-compensation. After releasing stock we still need to refund payment.
        require(order, OrderStatus.COMPENSATING, EventTypes.STOCK_RELEASED);
        commands.refundPayment(order);
    }

    private void onPaymentRefunded(Order order) {
        require(order, OrderStatus.COMPENSATING, EventTypes.PAYMENT_REFUNDED);
        // End of the rollback chain.
        transition(order, OrderStatus.FAILED);
    }

    // --------------------------------------------------------------------- //
    // Internals                                                             //
    // --------------------------------------------------------------------- //

    /**
     * Light-weight precondition: the order must be in the expected state for
     * a given event. Mismatches mean we dropped a message or the state machine
     * has a bug; in study code we'd rather fail loudly than silently corrupt.
     */
    private void require(Order order, OrderStatus expected, String eventType) {
        if (order.getStatus() != expected) {
            throw new IllegalStateException(
                    "Order " + order.getId() + " in state " + order.getStatus()
                            + " cannot accept event " + eventType + " (expected state " + expected + ")");
        }
    }

    private void transition(Order order, OrderStatus next) {
        OrderStatus prev = order.getStatus();
        order.transitionTo(next);
        log.info("Saga state {} -> {} saga_id={}", prev, next, order.getId());
    }
}
