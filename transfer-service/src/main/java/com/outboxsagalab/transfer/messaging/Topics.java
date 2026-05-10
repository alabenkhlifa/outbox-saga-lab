package com.outboxsagalab.transfer.messaging;

/**
 * Kafka topic names visible to transfer-service. The orchestrator publishes
 * commands to participants and consumes reply events from each of them.
 *
 * <p>See {@code docs/architecture.md} §6.
 */
public final class Topics {

    // Commands the orchestrator publishes (one per participant):
    public static final String ACCOUNT_COMMANDS      = "account.commands";
    public static final String FX_COMMANDS           = "fx.commands";
    public static final String NOTIFICATION_COMMANDS = "notification.commands";

    // Events the orchestrator publishes for observability:
    public static final String TRANSFER_EVENTS       = "transfer.events";

    // Events the orchestrator consumes (replies from each participant):
    public static final String ACCOUNT_EVENTS        = "account.events";
    public static final String FX_EVENTS             = "fx.events";
    public static final String NOTIFICATION_EVENTS   = "notification.events";

    private Topics() { /* utility */ }
}
