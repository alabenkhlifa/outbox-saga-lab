package com.outboxsagalab.payment.messaging;

/**
 * Kafka topic names for the payment-service.
 *
 * <p>The payment-service consumes commands from {@link #PAYMENT_COMMANDS}
 * and publishes events to {@link #PAYMENT_EVENTS}. See
 * {@code docs/architecture.md} section 5 for the full topic table.
 */
public final class Topics {

    public static final String PAYMENT_COMMANDS = "payment-commands";
    public static final String PAYMENT_EVENTS   = "payment-events";

    private Topics() {
        // utility class
    }
}
