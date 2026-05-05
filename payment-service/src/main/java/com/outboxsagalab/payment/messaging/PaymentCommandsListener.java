package com.outboxsagalab.payment.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.payment.domain.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Consumer for the {@code payment-commands} topic.
 *
 * <p>The listener is intentionally thin: it deserializes the canonical
 * envelope and dispatches to {@link PaymentService} based on
 * {@code event_type}. All business logic, idempotency, and the outbox
 * write live in the service so the transaction boundaries stay obvious.
 */
@Component
public class PaymentCommandsListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandsListener.class);

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    public PaymentCommandsListener(ObjectMapper objectMapper, PaymentService paymentService) {
        this.objectMapper = objectMapper;
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = "${spring.kafka.consumer.group-id}")
    public void onCommand(String rawMessage) {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(rawMessage, EventEnvelope.class);
        } catch (Exception e) {
            // Bad JSON — log and skip. Don't throw, or Spring Kafka will retry forever
            // on a poison pill. A real system would route to a DLQ here.
            log.error("failed to deserialize payment-commands message: {}", rawMessage, e);
            return;
        }

        log.info("received command saga_id={} event_type={} event_id={}",
                envelope.sagaId(), envelope.eventType(), envelope.eventId());

        switch (envelope.eventType()) {
            case "RequestPayment" -> handleRequestPayment(envelope);
            case "RefundPayment"  -> handleRefundPayment(envelope);
            default -> log.warn("ignoring unknown event_type={} on payment-commands", envelope.eventType());
        }
    }

    private void handleRequestPayment(EventEnvelope envelope) {
        JsonNode payload = envelope.payload();
        UUID orderId = UUID.fromString(payload.get("order_id").asText());
        BigDecimal amount = new BigDecimal(payload.get("amount").asText());
        String currency = payload.has("currency") ? payload.get("currency").asText() : "EUR";

        paymentService.chargeForOrder(envelope.eventId(), envelope.sagaId(), orderId, amount, currency);
    }

    private void handleRefundPayment(EventEnvelope envelope) {
        JsonNode payload = envelope.payload();
        UUID orderId = UUID.fromString(payload.get("order_id").asText());

        paymentService.refund(envelope.eventId(), envelope.sagaId(), orderId);
    }
}
