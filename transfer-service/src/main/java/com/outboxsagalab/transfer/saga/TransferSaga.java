package com.outboxsagalab.transfer.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.outboxsagalab.transfer.domain.Transfer;
import com.outboxsagalab.transfer.domain.TransferRepository;
import com.outboxsagalab.transfer.domain.TransferState;
import com.outboxsagalab.transfer.idempotency.ProcessedEvent;
import com.outboxsagalab.transfer.idempotency.ProcessedEventRepository;
import com.outboxsagalab.transfer.messaging.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Saga state machine for a cross-currency P2P transfer.
 *
 * <p>Single transactional entry point: idempotency check first, then routing
 * on event_type, then mark the inbound event as processed. Domain change +
 * outbox row + processed_event insert all commit together.
 *
 * <p>Happy path:
 * <pre>
 *   PENDING                -DebitAccount->  DEBIT_REQUESTED
 *   DEBIT_REQUESTED        <-AccountDebited- SENDER_DEBITED  -ConvertCurrency->  FX_REQUESTED
 *   FX_REQUESTED           <-CurrencyConverted- FX_CONVERTED  -CreditAccount->   CREDIT_REQUESTED
 *   CREDIT_REQUESTED       <-AccountCredited- RECIPIENT_CREDITED -SendNotification-> NOTIFICATION_REQUESTED
 *   NOTIFICATION_REQUESTED <-NotificationSent- COMPLETED
 * </pre>
 *
 * <p>Compensation chains: see spec §4.
 */
@Component
public class TransferSaga {

    private static final Logger log = LoggerFactory.getLogger(TransferSaga.class);

    private final TransferRepository transfers;
    private final ProcessedEventRepository processedEvents;
    private final CommandEmitter commands;

    public TransferSaga(TransferRepository transfers,
                        ProcessedEventRepository processedEvents,
                        CommandEmitter commands) {
        this.transfers = transfers;
        this.processedEvents = processedEvents;
        this.commands = commands;
    }

    @Transactional
    public void handle(EventEnvelope envelope) {
        UUID eventId = envelope.eventId();
        String type = envelope.eventType();
        UUID sagaId = envelope.sagaId();

        if (processedEvents.existsById(eventId)) {
            log.info("Skipping duplicate event event_id={} type={} saga_id={}", eventId, type, sagaId);
            return;
        }

        log.info("Saga consumed event_type={} event_id={} saga_id={}", type, eventId, sagaId);

        Transfer t = transfers.findById(sagaId)
                .orElseThrow(() -> new IllegalStateException(
                        "Unknown saga_id=" + sagaId + " for event_id=" + eventId));

        switch (type) {
            case EventTypes.ACCOUNT_DEBITED      -> onAccountDebited(t);
            case EventTypes.DEBIT_REJECTED       -> onDebitRejected(t);
            case EventTypes.CURRENCY_CONVERTED   -> onCurrencyConverted(t, envelope.payload());
            case EventTypes.CONVERSION_REJECTED  -> onConversionRejected(t);
            case EventTypes.ACCOUNT_CREDITED     -> onAccountCredited(t);
            case EventTypes.CREDIT_REJECTED      -> onCreditRejected(t);
            case EventTypes.FX_REVERSED          -> onFxReversed(t);
            case EventTypes.DEBIT_REFUNDED       -> onDebitRefunded(t);
            case EventTypes.NOTIFICATION_SENT    -> onNotificationSent(t);
            default -> log.warn("Saga ignoring unknown event_type={} event_id={} saga_id={}",
                    type, eventId, sagaId);
        }

        processedEvents.save(new ProcessedEvent(eventId, type));
    }

    // -------- happy-path handlers --------

    private void onAccountDebited(Transfer t) {
        require(t, TransferState.DEBIT_REQUESTED, EventTypes.ACCOUNT_DEBITED);
        transition(t, TransferState.SENDER_DEBITED);
        commands.convertCurrency(t);
        transition(t, TransferState.FX_REQUESTED);
    }

    private void onCurrencyConverted(Transfer t, JsonNode payload) {
        require(t, TransferState.FX_REQUESTED, EventTypes.CURRENCY_CONVERTED);
        BigDecimal quoteAmount = new BigDecimal(payload.get("quote_amount").asText());
        BigDecimal rate = new BigDecimal(payload.get("rate").asText());
        t.recordFx(quoteAmount, rate);
        transition(t, TransferState.FX_CONVERTED);
        commands.creditAccount(t);
        transition(t, TransferState.CREDIT_REQUESTED);
    }

    private void onAccountCredited(Transfer t) {
        require(t, TransferState.CREDIT_REQUESTED, EventTypes.ACCOUNT_CREDITED);
        transition(t, TransferState.RECIPIENT_CREDITED);
        commands.sendNotification(t, "TRANSFER_SENT");
        transition(t, TransferState.NOTIFICATION_REQUESTED);
    }

    private void onNotificationSent(Transfer t) {
        require(t, TransferState.NOTIFICATION_REQUESTED, EventTypes.NOTIFICATION_SENT);
        transition(t, TransferState.COMPLETED);
    }

    // -------- failure handlers --------

    private void onDebitRejected(Transfer t) {
        require(t, TransferState.DEBIT_REQUESTED, EventTypes.DEBIT_REJECTED);
        // Nothing happened — terminal failure.
        transition(t, TransferState.FAILED);
    }

    private void onConversionRejected(Transfer t) {
        require(t, TransferState.FX_REQUESTED, EventTypes.CONVERSION_REJECTED);
        // Sender already debited — refund to roll back.
        transition(t, TransferState.COMPENSATING);
        commands.refundAccount(t);
    }

    private void onCreditRejected(Transfer t) {
        require(t, TransferState.CREDIT_REQUESTED, EventTypes.CREDIT_REJECTED);
        // Reverse FX first, then refund the original debit (handled when FxReversed arrives).
        transition(t, TransferState.COMPENSATING);
        commands.reverseConversion(t);
    }

    private void onFxReversed(Transfer t) {
        // Mid-compensation only.
        require(t, TransferState.COMPENSATING, EventTypes.FX_REVERSED);
        commands.refundAccount(t);
    }

    private void onDebitRefunded(Transfer t) {
        // Terminal step of any compensation chain that reached the refund stage.
        require(t, TransferState.COMPENSATING, EventTypes.DEBIT_REFUNDED);
        transition(t, TransferState.FAILED);
    }

    // -------- helpers --------

    private void require(Transfer t, TransferState expected, String eventType) {
        if (t.getState() != expected) {
            throw new IllegalStateException(
                    "Transfer " + t.getId() + " in state " + t.getState()
                            + " cannot accept event " + eventType + " (expected " + expected + ")");
        }
    }

    private void transition(Transfer t, TransferState next) {
        TransferState prev = t.getState();
        t.transitionTo(next);
        log.info("Saga state {} -> {} saga_id={}", prev, next, t.getId());
    }
}
