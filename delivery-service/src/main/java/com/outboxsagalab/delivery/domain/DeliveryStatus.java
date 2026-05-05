package com.outboxsagalab.delivery.domain;

/**
 * Lifecycle of a {@link Delivery}.
 *
 * <p>This service is a saga participant; the states reflect the outcomes
 * we report back on the {@code delivery-events} topic.
 */
public enum DeliveryStatus {
    /** Successfully scheduled. We have a {@code scheduled_at} and have emitted DeliveryScheduled. */
    SCHEDULED,

    /** The stub decided this delivery cannot be scheduled (~5% of cases). DeliveryFailed emitted. */
    FAILED,

    /** Cancelled in response to a CancelDelivery command. DeliveryCancelled emitted. */
    CANCELLED
}
