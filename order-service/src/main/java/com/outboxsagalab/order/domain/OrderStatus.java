package com.outboxsagalab.order.domain;

/**
 * Saga states (see docs/architecture.md section 5 "Saga states").
 *
 *   PENDING            -> RequestPayment sent
 *   PAYMENT_REQUESTED  -> waiting for PaymentApproved/Declined
 *   PAID               -> ReserveStock sent
 *   STOCK_REQUESTED    -> waiting for StockReserved/Failed
 *   RESERVED           -> ScheduleDelivery sent
 *   DELIVERY_REQUESTED -> waiting for DeliveryScheduled/Failed
 *   COMPLETED          -> terminal success
 *   FAILED             -> terminal failure
 *   COMPENSATING       -> mid-rollback (sub-states optional)
 */
public enum OrderStatus {
    PENDING,
    PAYMENT_REQUESTED,
    PAID,
    STOCK_REQUESTED,
    RESERVED,
    DELIVERY_REQUESTED,
    COMPLETED,
    FAILED,
    COMPENSATING
}
