package com.outboxsagalab.transfer.domain;

/**
 * Saga state machine for a transfer. Mirrors the *_REQUESTED -> *_DONE
 * waiting-state convention used in {@code docs/architecture.md}.
 */
public enum TransferState {
    PENDING,
    DEBIT_REQUESTED,
    SENDER_DEBITED,
    FX_REQUESTED,
    FX_CONVERTED,
    CREDIT_REQUESTED,
    RECIPIENT_CREDITED,
    NOTIFICATION_REQUESTED,
    COMPLETED,
    COMPENSATING,
    FAILED
}
