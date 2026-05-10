package com.outboxsagalab.transfer.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.transfer.saga.TransferSaga;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AccountEventsListener {

    private static final Logger log = LoggerFactory.getLogger(AccountEventsListener.class);

    private final ObjectMapper json;
    private final TransferSaga saga;

    public AccountEventsListener(ObjectMapper json, TransferSaga saga) {
        this.json = json;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.ACCOUNT_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String raw) {
        EventEnvelope envelope;
        try {
            envelope = json.readValue(raw, EventEnvelope.class);
        } catch (Exception e) {
            log.error("Bad envelope on {}: {}", Topics.ACCOUNT_EVENTS, raw, e);
            return;
        }
        saga.handle(envelope);
    }
}
