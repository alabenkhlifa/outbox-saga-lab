package com.outboxsagalab.inventory.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.inventory.domain.InventoryService;
import com.outboxsagalab.inventory.domain.ReservationItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Kafka entry point for inbound commands. Parses the standard envelope and
 * dispatches to {@link InventoryService} based on {@code event_type}.
 *
 * Note: the {@code @Transactional} boundary lives on {@link InventoryService}
 * so that the idempotency check, domain change, and outbox insert all share
 * the same transaction. This listener is intentionally thin.
 */
@Component
public class InventoryCommandsListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandsListener.class);

    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    public InventoryCommandsListener(InventoryService inventoryService, ObjectMapper objectMapper) {
        this.inventoryService = inventoryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String raw) {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(raw, EventEnvelope.class);
        } catch (Exception e) {
            log.error("Dropping malformed inventory-commands message: {}", e.toString());
            return;
        }

        log.info("Consumed inventory-command event_id={} type={} saga_id={}",
                envelope.eventId(), envelope.eventType(), envelope.sagaId());

        switch (envelope.eventType()) {
            case "ReserveStock" -> handleReserve(envelope);
            case "ReleaseStock" -> handleRelease(envelope);
            default -> log.warn("Ignoring unknown event_type='{}' on inventory-commands", envelope.eventType());
        }
    }

    private void handleReserve(EventEnvelope envelope) {
        JsonNode payload = envelope.payload();
        UUID orderId = UUID.fromString(payload.get("order_id").asText());

        List<ReservationItem> items = new ArrayList<>();
        JsonNode itemsNode = payload.get("items");
        if (itemsNode != null && itemsNode.isArray()) {
            for (JsonNode line : itemsNode) {
                items.add(new ReservationItem(
                        line.get("sku").asText(),
                        line.get("qty").asInt()
                ));
            }
        }
        inventoryService.reserveForOrder(envelope.eventId(), envelope.sagaId(), orderId, items);
    }

    private void handleRelease(EventEnvelope envelope) {
        JsonNode payload = envelope.payload();
        UUID orderId = UUID.fromString(payload.get("order_id").asText());
        inventoryService.releaseForOrder(envelope.eventId(), envelope.sagaId(), orderId);
    }
}
