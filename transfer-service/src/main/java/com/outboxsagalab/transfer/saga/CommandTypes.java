package com.outboxsagalab.transfer.saga;

/**
 * Constants for outbound command event_type values written to the outbox.
 * See spec §6.
 */
public final class CommandTypes {

    public static final String DEBIT_ACCOUNT       = "DebitAccount";
    public static final String CREDIT_ACCOUNT      = "CreditAccount";
    public static final String REFUND_ACCOUNT      = "RefundAccount";
    public static final String CONVERT_CURRENCY    = "ConvertCurrency";
    public static final String REVERSE_CONVERSION  = "ReverseConversion";
    public static final String SEND_NOTIFICATION   = "SendNotification";

    private CommandTypes() { /* utility */ }
}
