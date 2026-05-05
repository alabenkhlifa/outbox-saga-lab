package com.outboxsagalab.order.messaging;

/**
 * Single source of truth for Kafka topic names.
 *
 * order-service:
 *   - PRODUCES to *_COMMANDS topics (it tells participants what to do)
 *   - CONSUMES from *_EVENTS topics (it reacts to participant replies)
 */
public final class Topics {

    private Topics() {
        // no instances
    }

    // Commands: order -> participants
    public static final String PAYMENT_COMMANDS   = "payment-commands";
    public static final String INVENTORY_COMMANDS = "inventory-commands";
    public static final String DELIVERY_COMMANDS  = "delivery-commands";

    // Events: participants -> order
    public static final String PAYMENT_EVENTS     = "payment-events";
    public static final String INVENTORY_EVENTS   = "inventory-events";
    public static final String DELIVERY_EVENTS    = "delivery-events";
}
