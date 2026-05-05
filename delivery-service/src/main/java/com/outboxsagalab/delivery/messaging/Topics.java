package com.outboxsagalab.delivery.messaging;

/**
 * Kafka topic names used by the delivery-service.
 *
 * <p>The orchestrator (order-service) publishes commands on
 * {@link #DELIVERY_COMMANDS}; we publish reply events on
 * {@link #DELIVERY_EVENTS}. The topic names are part of the cross-service
 * contract — see {@code docs/architecture.md}.
 */
public final class Topics {

    /** Inbound: commands from the saga orchestrator (ScheduleDelivery, CancelDelivery). */
    public static final String DELIVERY_COMMANDS = "delivery-commands";

    /** Outbound: reply events emitted by this service (DeliveryScheduled, DeliveryFailed, DeliveryCancelled). */
    public static final String DELIVERY_EVENTS = "delivery-events";

    private Topics() {
        // utility class
    }
}
