package com.outboxsagalab.order.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.outboxsagalab.order.domain.Order;
import com.outboxsagalab.order.messaging.Topics;
import com.outboxsagalab.order.outbox.OutboxEntry;
import com.outboxsagalab.order.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Tiny helper that knows how to drop a "command" row into the outbox.
 *
 * The saga calls this; the saga does not touch Kafka. That keeps the
 * "domain change + outbox row in same TX" invariant trivially obvious.
 *
 * Each method writes ONE outbox row in the saga's current transaction.
 */
@Component
public class CommandEmitter {

    private static final Logger log = LoggerFactory.getLogger(CommandEmitter.class);

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public CommandEmitter(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    public void requestPayment(Order order) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("order_id", order.getId().toString());
        payload.put("customer_id", order.getCustomerId());
        payload.put("amount", order.getTotalAmount());
        emit(order.getId(), Topics.PAYMENT_COMMANDS, CommandTypes.REQUEST_PAYMENT, payload);
    }

    public void refundPayment(Order order) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("order_id", order.getId().toString());
        payload.put("amount", order.getTotalAmount());
        emit(order.getId(), Topics.PAYMENT_COMMANDS, CommandTypes.REFUND_PAYMENT, payload);
    }

    public void reserveStock(Order order) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("order_id", order.getId().toString());
        ObjectNode itemsNode = payload.putObject("items");
        order.getItems().forEach(item ->
                itemsNode.put(item.getSku(), item.getQuantity()));
        emit(order.getId(), Topics.INVENTORY_COMMANDS, CommandTypes.RESERVE_STOCK, payload);
    }

    public void releaseStock(Order order) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("order_id", order.getId().toString());
        ObjectNode itemsNode = payload.putObject("items");
        order.getItems().forEach(item ->
                itemsNode.put(item.getSku(), item.getQuantity()));
        emit(order.getId(), Topics.INVENTORY_COMMANDS, CommandTypes.RELEASE_STOCK, payload);
    }

    public void scheduleDelivery(Order order) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("order_id", order.getId().toString());
        payload.put("customer_id", order.getCustomerId());
        emit(order.getId(), Topics.DELIVERY_COMMANDS, CommandTypes.SCHEDULE_DELIVERY, payload);
    }

    private void emit(UUID sagaId, String topic, String eventType, JsonNode payload) {
        UUID eventId = UUID.randomUUID();
        OutboxEntry entry = new OutboxEntry(
                eventId,
                sagaId.toString(),
                topic,
                eventType,
                payload
        );
        outboxRepository.save(entry);
        log.info("Outbox <- enqueued topic={} event_type={} event_id={} saga_id={}",
                topic, eventType, eventId, sagaId);
    }

    // -- helpers used by JSON building, kept as overloads on ObjectNode -- //

    private static void put(ObjectNode node, String field, BigDecimal value) {
        node.put(field, value);
    }
}
