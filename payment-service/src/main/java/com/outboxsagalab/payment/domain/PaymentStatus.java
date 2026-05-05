package com.outboxsagalab.payment.domain;

/**
 * Lifecycle states of a Payment row.
 *
 * <p>This service is intentionally simple — a payment is either approved,
 * declined, or refunded. There is no "pending" state because the stub
 * charge logic decides synchronously inside the transaction.
 */
public enum PaymentStatus {
    APPROVED,
    DECLINED,
    REFUNDED
}
