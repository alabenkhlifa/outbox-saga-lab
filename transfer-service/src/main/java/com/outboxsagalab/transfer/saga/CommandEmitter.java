package com.outboxsagalab.transfer.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.outboxsagalab.transfer.domain.Transfer;
import com.outboxsagalab.transfer.messaging.Topics;
import com.outboxsagalab.transfer.outbox.OutboxEntry;
import com.outboxsagalab.transfer.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Writes outbound commands as outbox rows. The saga calls this in its
 * transaction; the outbox poller drains rows to Kafka separately.
 */
@Component
public class CommandEmitter {

    private static final Logger log = LoggerFactory.getLogger(CommandEmitter.class);

    private final OutboxRepository outbox;
    private final ObjectMapper json;

    public CommandEmitter(OutboxRepository outbox, ObjectMapper json) {
        this.outbox = outbox;
        this.json = json;
    }

    public void debitAccount(Transfer t) {
        ObjectNode p = json.createObjectNode();
        p.put("transfer_id", t.getId().toString());
        p.put("user", t.getSenderUser());
        p.put("currency", t.getSenderCurrency());
        p.put("amount", t.getSourceAmount());
        emit(t.getId(), Topics.ACCOUNT_COMMANDS, CommandTypes.DEBIT_ACCOUNT, p);
    }

    public void refundAccount(Transfer t) {
        ObjectNode p = json.createObjectNode();
        p.put("transfer_id", t.getId().toString());
        p.put("user", t.getSenderUser());
        p.put("currency", t.getSenderCurrency());
        p.put("amount", t.getSourceAmount());
        emit(t.getId(), Topics.ACCOUNT_COMMANDS, CommandTypes.REFUND_ACCOUNT, p);
    }

    public void creditAccount(Transfer t) {
        ObjectNode p = json.createObjectNode();
        p.put("transfer_id", t.getId().toString());
        p.put("user", t.getRecipientUser());
        p.put("currency", t.getRecipientCurrency());
        p.put("amount", t.getTargetAmount());
        emit(t.getId(), Topics.ACCOUNT_COMMANDS, CommandTypes.CREDIT_ACCOUNT, p);
    }

    public void convertCurrency(Transfer t) {
        ObjectNode p = json.createObjectNode();
        p.put("transfer_id", t.getId().toString());
        p.put("base_currency", t.getSenderCurrency());
        p.put("quote_currency", t.getRecipientCurrency());
        p.put("base_amount", t.getSourceAmount());
        emit(t.getId(), Topics.FX_COMMANDS, CommandTypes.CONVERT_CURRENCY, p);
    }

    public void reverseConversion(Transfer t) {
        ObjectNode p = json.createObjectNode();
        p.put("transfer_id", t.getId().toString());
        emit(t.getId(), Topics.FX_COMMANDS, CommandTypes.REVERSE_CONVERSION, p);
    }

    public void sendNotification(Transfer t, String kind) {
        ObjectNode p = json.createObjectNode();
        p.put("transfer_id", t.getId().toString());
        p.put("kind", kind);
        p.put("sender_user", t.getSenderUser());
        p.put("recipient_user", t.getRecipientUser());
        p.put("sender_currency", t.getSenderCurrency());
        p.put("recipient_currency", t.getRecipientCurrency());
        p.put("source_amount", t.getSourceAmount());
        if (t.getTargetAmount() != null) {
            p.put("target_amount", t.getTargetAmount());
        }
        emit(t.getId(), Topics.NOTIFICATION_COMMANDS, CommandTypes.SEND_NOTIFICATION, p);
    }

    private void emit(UUID sagaId, String topic, String eventType, JsonNode payload) {
        UUID eventId = UUID.randomUUID();
        OutboxEntry entry = new OutboxEntry(eventId, sagaId.toString(), topic, eventType, payload);
        outbox.save(entry);
        log.info("Outbox <- enqueued topic={} event_type={} event_id={} saga_id={}",
                topic, eventType, eventId, sagaId);
    }
}
