package com.outboxsagalab.order.saga;

/**
 * Inbound event names — what participants reply with.
 * Matches the names used in the architecture diagrams.
 */
public final class EventTypes {

    private EventTypes() {
    }

    public static final String PAYMENT_APPROVED          = "PaymentApproved";
    public static final String PAYMENT_DECLINED          = "PaymentDeclined";
    public static final String PAYMENT_REFUNDED          = "PaymentRefunded";

    public static final String STOCK_RESERVED            = "StockReserved";
    public static final String STOCK_RESERVATION_FAILED  = "StockReservationFailed";
    public static final String STOCK_RELEASED            = "StockReleased";

    public static final String DELIVERY_SCHEDULED        = "DeliveryScheduled";
    public static final String DELIVERY_FAILED           = "DeliveryFailed";
}
