package com.outboxsagalab.account.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.outboxsagalab.account.idempotency.ProcessedEvent;
import com.outboxsagalab.account.idempotency.ProcessedEventRepository;
import com.outboxsagalab.account.messaging.Topics;
import com.outboxsagalab.account.outbox.OutboxEntry;
import com.outboxsagalab.account.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Domain service for {@code DebitAccount}, {@code CreditAccount}, and
 * {@code RefundAccount} commands.
 *
 * <p>Each public method is one {@code @Transactional} unit following the
 * idempotent-saga-participant recipe:
 *
 * <ol>
 *   <li>Idempotency check on the inbound event_id.</li>
 *   <li>Domain work (wallet balance update + movement row).</li>
 *   <li>Mark the event processed.</li>
 *   <li>Enqueue the reply event in the outbox.</li>
 * </ol>
 *
 * <p>If the wallet doesn't exist or the debit would overdraw, we emit a
 * rejected event and the saga compensates / fails accordingly.
 */
@Service
public class AccountService {

    private static final Logger log = LoggerFactory.getLogger(AccountService.class);

    private final WalletRepository wallets;
    private final WalletMovementRepository movements;
    private final OutboxRepository outbox;
    private final ProcessedEventRepository processedEvents;
    private final ObjectMapper json;

    public AccountService(WalletRepository wallets,
                          WalletMovementRepository movements,
                          OutboxRepository outbox,
                          ProcessedEventRepository processedEvents,
                          ObjectMapper json) {
        this.wallets = wallets;
        this.movements = movements;
        this.outbox = outbox;
        this.processedEvents = processedEvents;
        this.json = json;
    }

    @Transactional
    public void debit(UUID inboundEventId, UUID transferId,
                      String userId, String currency, BigDecimal amount) {
        if (alreadyProcessed(inboundEventId, "DebitAccount", transferId)) return;

        var maybeWallet = wallets.findByUserIdAndCurrency(userId, currency);
        if (maybeWallet.isEmpty() || !maybeWallet.get().canDebit(amount)) {
            log.info("Debit rejected transfer_id={} user={} {} amount={}",
                    transferId, userId, currency, amount);
            processedEvents.save(new ProcessedEvent(inboundEventId, "DebitAccount"));
            ObjectNode reject = json.createObjectNode();
            reject.put("transfer_id", transferId.toString());
            reject.put("reason", maybeWallet.isEmpty() ? "WALLET_MISSING" : "INSUFFICIENT_FUNDS");
            enqueue(transferId, "DebitRejected", reject);
            return;
        }

        Wallet w = maybeWallet.get();
        w.debit(amount);
        wallets.save(w);
        movements.save(new WalletMovement(UUID.randomUUID(), w.getId(), amount,
                WalletMovementType.DEBIT, transferId));
        processedEvents.save(new ProcessedEvent(inboundEventId, "DebitAccount"));

        ObjectNode ok = json.createObjectNode();
        ok.put("transfer_id", transferId.toString());
        ok.put("user", userId);
        ok.put("currency", currency);
        ok.put("amount", amount);
        ok.put("new_balance", w.getBalance());
        enqueue(transferId, "AccountDebited", ok);

        log.info("Debited transfer_id={} user={} {} amount={} new_balance={}",
                transferId, userId, currency, amount, w.getBalance());
    }

    @Transactional
    public void credit(UUID inboundEventId, UUID transferId,
                       String userId, String currency, BigDecimal amount) {
        if (alreadyProcessed(inboundEventId, "CreditAccount", transferId)) return;

        var maybeWallet = wallets.findByUserIdAndCurrency(userId, currency);
        if (maybeWallet.isEmpty()) {
            log.info("Credit rejected (wallet missing) transfer_id={} user={} {}",
                    transferId, userId, currency);
            processedEvents.save(new ProcessedEvent(inboundEventId, "CreditAccount"));
            ObjectNode reject = json.createObjectNode();
            reject.put("transfer_id", transferId.toString());
            reject.put("reason", "WALLET_MISSING");
            enqueue(transferId, "CreditRejected", reject);
            return;
        }

        Wallet w = maybeWallet.get();
        w.credit(amount);
        wallets.save(w);
        movements.save(new WalletMovement(UUID.randomUUID(), w.getId(), amount,
                WalletMovementType.CREDIT, transferId));
        processedEvents.save(new ProcessedEvent(inboundEventId, "CreditAccount"));

        ObjectNode ok = json.createObjectNode();
        ok.put("transfer_id", transferId.toString());
        ok.put("user", userId);
        ok.put("currency", currency);
        ok.put("amount", amount);
        ok.put("new_balance", w.getBalance());
        enqueue(transferId, "AccountCredited", ok);

        log.info("Credited transfer_id={} user={} {} amount={} new_balance={}",
                transferId, userId, currency, amount, w.getBalance());
    }

    @Transactional
    public void refund(UUID inboundEventId, UUID transferId,
                       String userId, String currency, BigDecimal amount) {
        if (alreadyProcessed(inboundEventId, "RefundAccount", transferId)) return;

        Wallet w = wallets.findByUserIdAndCurrency(userId, currency)
                .orElseThrow(() -> new IllegalStateException(
                        "Refund target wallet missing user=" + userId + " " + currency));
        w.credit(amount);
        wallets.save(w);
        movements.save(new WalletMovement(UUID.randomUUID(), w.getId(), amount,
                WalletMovementType.REFUND, transferId));
        processedEvents.save(new ProcessedEvent(inboundEventId, "RefundAccount"));

        ObjectNode ok = json.createObjectNode();
        ok.put("transfer_id", transferId.toString());
        ok.put("user", userId);
        ok.put("currency", currency);
        ok.put("amount", amount);
        ok.put("new_balance", w.getBalance());
        enqueue(transferId, "DebitRefunded", ok);

        log.info("Refunded transfer_id={} user={} {} amount={}", transferId, userId, currency, amount);
    }

    private boolean alreadyProcessed(UUID eventId, String type, UUID sagaId) {
        if (processedEvents.existsById(eventId)) {
            log.info("Skipping already-processed {} event_id={} saga_id={}", type, eventId, sagaId);
            return true;
        }
        return false;
    }

    private void enqueue(UUID sagaId, String eventType, ObjectNode innerPayload) {
        UUID outboundEventId = UUID.randomUUID();
        ObjectNode envelope = json.createObjectNode();
        envelope.put("event_id", outboundEventId.toString());
        envelope.put("event_type", eventType);
        envelope.put("saga_id", sagaId.toString());
        envelope.put("occurred_at", Instant.now().toString());
        envelope.set("payload", innerPayload);

        String body;
        try {
            body = json.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize outbox envelope", e);
        }

        outbox.save(new OutboxEntry(outboundEventId, sagaId.toString(),
                Topics.ACCOUNT_EVENTS, eventType, body));
        log.info("Enqueued outbox event saga_id={} event_type={} event_id={}",
                sagaId, eventType, outboundEventId);
    }
}
