package com.outboxsagalab.order.saga;

/**
 * Outbound command names — what order-service tells participants to do.
 * Matches the names used in the architecture diagrams.
 */
public final class CommandTypes {

    private CommandTypes() {
    }

    public static final String REQUEST_PAYMENT   = "RequestPayment";
    public static final String REFUND_PAYMENT    = "RefundPayment";

    public static final String RESERVE_STOCK     = "ReserveStock";
    public static final String RELEASE_STOCK     = "ReleaseStock";

    public static final String SCHEDULE_DELIVERY = "ScheduleDelivery";
}
