package com.outboxsagalab.account.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.account.domain.AccountService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class AccountCommandsListener {

    private static final Logger log = LoggerFactory.getLogger(AccountCommandsListener.class);

    private final ObjectMapper json;
    private final AccountService accounts;

    public AccountCommandsListener(ObjectMapper json, AccountService accounts) {
        this.json = json;
        this.accounts = accounts;
    }

    @KafkaListener(topics = Topics.ACCOUNT_COMMANDS, groupId = "${spring.kafka.consumer.group-id}")
    public void onCommand(String raw) {
        EventEnvelope envelope;
        try {
            envelope = json.readValue(raw, EventEnvelope.class);
        } catch (Exception e) {
            log.error("Bad envelope on {}: {}", Topics.ACCOUNT_COMMANDS, raw, e);
            return;
        }

        log.info("Received command saga_id={} event_type={} event_id={}",
                envelope.sagaId(), envelope.eventType(), envelope.eventId());

        JsonNode p = envelope.payload();
        UUID transferId = UUID.fromString(p.get("transfer_id").asText());
        String user = p.get("user").asText();
        String currency = p.get("currency").asText();
        BigDecimal amount = new BigDecimal(p.get("amount").asText());

        switch (envelope.eventType()) {
            case "DebitAccount"  -> accounts.debit(envelope.eventId(), transferId, user, currency, amount);
            case "CreditAccount" -> accounts.credit(envelope.eventId(), transferId, user, currency, amount);
            case "RefundAccount" -> accounts.refund(envelope.eventId(), transferId, user, currency, amount);
            default -> log.warn("Ignoring unknown event_type={} on {}",
                    envelope.eventType(), Topics.ACCOUNT_COMMANDS);
        }
    }
}
