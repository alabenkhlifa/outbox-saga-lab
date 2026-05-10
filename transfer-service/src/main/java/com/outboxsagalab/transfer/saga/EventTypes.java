package com.outboxsagalab.transfer.saga;

/**
 * Constants for inbound reply event_type values consumed by the saga.
 * See spec §6.
 */
public final class EventTypes {

    public static final String ACCOUNT_DEBITED          = "AccountDebited";
    public static final String DEBIT_REJECTED           = "DebitRejected";
    public static final String DEBIT_REFUNDED           = "DebitRefunded";

    public static final String ACCOUNT_CREDITED         = "AccountCredited";
    public static final String CREDIT_REJECTED          = "CreditRejected";

    public static final String CURRENCY_CONVERTED       = "CurrencyConverted";
    public static final String CONVERSION_REJECTED      = "ConversionRejected";
    public static final String FX_REVERSED              = "FxReversed";

    public static final String NOTIFICATION_SENT        = "NotificationSent";

    private EventTypes() { /* utility */ }
}
