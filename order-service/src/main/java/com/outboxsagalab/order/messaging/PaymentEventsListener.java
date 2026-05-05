package com.outboxsagalab.order.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.order.saga.OrderSaga;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Thin listener — deserializes the envelope and hands it to the saga.
 * No business logic lives here.
 */
@Component
public class PaymentEventsListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventsListener.class);

    private final ObjectMapper objectMapper;
    private final OrderSaga saga;

    public PaymentEventsListener(ObjectMapper objectMapper, OrderSaga saga) {
        this.objectMapper = objectMapper;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String payload) {
        try {
            EventEnvelope envelope = objectMapper.readValue(payload, EventEnvelope.class);
            log.info("Kafka -> Listener topic={} event_type={} event_id={} saga_id={}",
                    Topics.PAYMENT_EVENTS, envelope.eventType(), envelope.eventId(), envelope.sagaId());
            saga.handle(envelope);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize payment event payload={}", payload, e);
            // In a real system this would go to a DLQ; the lab settles for a loud log.
        }
    }
}
