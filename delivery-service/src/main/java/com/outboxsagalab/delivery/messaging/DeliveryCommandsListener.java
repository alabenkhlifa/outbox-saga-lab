package com.outboxsagalab.delivery.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.delivery.domain.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Inbound Kafka listener for {@link Topics#DELIVERY_COMMANDS}.
 *
 * <p>Deserialises the {@link EventEnvelope}, dispatches on
 * {@code event_type}, and delegates to {@link DeliveryService} where the
 * idempotency check happens. The listener is intentionally thin — all
 * transactional work lives in the service.
 */
@Component
public class DeliveryCommandsListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryCommandsListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public DeliveryCommandsListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.DELIVERY_COMMANDS, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String raw) {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(raw, EventEnvelope.class);
        } catch (Exception e) {
            // Poison message — log and swallow so we don't block the partition.
            log.error("delivery could not parse envelope on {} payload={}", Topics.DELIVERY_COMMANDS, raw, e);
            return;
        }

        log.info("delivery consumed event_id={} event_type={} saga_id={}",
                envelope.eventId(), envelope.eventType(), envelope.sagaId());

        switch (Objects.toString(envelope.eventType(), "")) {
            case "ScheduleDelivery" -> handleSchedule(envelope);
            case "CancelDelivery"   -> handleCancel(envelope);
            default -> log.warn("delivery ignoring unknown event_type={} event_id={}",
                    envelope.eventType(), envelope.eventId());
        }
    }

    private void handleSchedule(EventEnvelope envelope) {
        Map<String, Object> payload = envelope.payload();
        if (payload == null) {
            log.error("delivery ScheduleDelivery missing payload event_id={}", envelope.eventId());
            return;
        }
        UUID orderId = parseUuid(payload.get("order_id"), "order_id", envelope.eventId());
        if (orderId == null) {
            return;
        }
        String address = Objects.toString(payload.get("address"), "");
        deliveryService.scheduleForOrder(
                envelope.eventId(),
                envelope.eventType(),
                envelope.sagaId(),
                orderId,
                address
        );
    }

    private void handleCancel(EventEnvelope envelope) {
        Map<String, Object> payload = envelope.payload();
        if (payload == null) {
            log.error("delivery CancelDelivery missing payload event_id={}", envelope.eventId());
            return;
        }
        UUID orderId = parseUuid(payload.get("order_id"), "order_id", envelope.eventId());
        if (orderId == null) {
            return;
        }
        deliveryService.cancelForOrder(
                envelope.eventId(),
                envelope.eventType(),
                envelope.sagaId(),
                orderId
        );
    }

    private static UUID parseUuid(Object raw, String field, UUID eventId) {
        if (raw == null) {
            log.error("delivery missing required field={} event_id={}", field, eventId);
            return null;
        }
        try {
            return UUID.fromString(raw.toString());
        } catch (IllegalArgumentException e) {
            log.error("delivery invalid UUID for field={} value={} event_id={}", field, raw, eventId);
            return null;
        }
    }
}
