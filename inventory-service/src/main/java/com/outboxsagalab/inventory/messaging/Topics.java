package com.outboxsagalab.inventory.messaging;

/**
 * Kafka topic names this service consumes from / produces to.
 * The lab uses single-partition topics — see docs/architecture.md §5.
 */
public final class Topics {

    /** Inbound commands from the order-service (ReserveStock, ReleaseStock). */
    public static final String INVENTORY_COMMANDS = "inventory-commands";

    /** Outbound events to the order-service (StockReserved, StockReservationFailed, StockReleased). */
    public static final String INVENTORY_EVENTS = "inventory-events";

    private Topics() {
    }
}
