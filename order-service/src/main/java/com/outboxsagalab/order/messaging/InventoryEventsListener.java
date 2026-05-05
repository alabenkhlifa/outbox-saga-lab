package com.outboxsagalab.order.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.order.saga.OrderSaga;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventsListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventsListener.class);

    private final ObjectMapper objectMapper;
    private final OrderSaga saga;

    public InventoryEventsListener(ObjectMapper objectMapper, OrderSaga saga) {
        this.objectMapper = objectMapper;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.INVENTORY_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String payload) {
        try {
            EventEnvelope envelope = objectMapper.readValue(payload, EventEnvelope.class);
            log.info("Kafka -> Listener topic={} event_type={} event_id={} saga_id={}",
                    Topics.INVENTORY_EVENTS, envelope.eventType(), envelope.eventId(), envelope.sagaId());
            saga.handle(envelope);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize inventory event payload={}", payload, e);
        }
    }
}
