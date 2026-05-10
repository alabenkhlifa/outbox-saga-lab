package com.outboxsagalab.fx.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.fx.domain.FxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class FxCommandsListener {

    private static final Logger log = LoggerFactory.getLogger(FxCommandsListener.class);

    private final ObjectMapper json;
    private final FxService fx;

    public FxCommandsListener(ObjectMapper json, FxService fx) {
        this.json = json;
        this.fx = fx;
    }

    @KafkaListener(topics = Topics.FX_COMMANDS, groupId = "${spring.kafka.consumer.group-id}")
    public void onCommand(String raw) {
        EventEnvelope envelope;
        try {
            envelope = json.readValue(raw, EventEnvelope.class);
        } catch (Exception e) {
            log.error("Bad envelope on {}: {}", Topics.FX_COMMANDS, raw, e);
            return;
        }

        log.info("Received command saga_id={} event_type={} event_id={}",
                envelope.sagaId(), envelope.eventType(), envelope.eventId());

        JsonNode p = envelope.payload();
        UUID transferId = UUID.fromString(p.get("transfer_id").asText());

        switch (envelope.eventType()) {
            case "ConvertCurrency" -> {
                String base = p.get("base_currency").asText();
                String quote = p.get("quote_currency").asText();
                BigDecimal baseAmount = new BigDecimal(p.get("base_amount").asText());
                fx.convert(envelope.eventId(), transferId, base, quote, baseAmount);
            }
            case "ReverseConversion" -> fx.reverse(envelope.eventId(), transferId);
            default -> log.warn("Ignoring unknown event_type={} on {}",
                    envelope.eventType(), Topics.FX_COMMANDS);
        }
    }
}
