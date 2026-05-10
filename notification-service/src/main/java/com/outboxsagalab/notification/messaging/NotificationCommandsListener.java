package com.outboxsagalab.notification.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.outboxsagalab.notification.domain.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class NotificationCommandsListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationCommandsListener.class);

    private final ObjectMapper json;
    private final NotificationService notifications;

    public NotificationCommandsListener(ObjectMapper json, NotificationService notifications) {
        this.json = json;
        this.notifications = notifications;
    }

    @KafkaListener(topics = Topics.NOTIFICATION_COMMANDS, groupId = "${spring.kafka.consumer.group-id}")
    public void onCommand(String raw) {
        EventEnvelope envelope;
        try {
            envelope = json.readValue(raw, EventEnvelope.class);
        } catch (Exception e) {
            log.error("Bad envelope on {}: {}", Topics.NOTIFICATION_COMMANDS, raw, e);
            return;
        }

        log.info("Received command saga_id={} event_type={} event_id={}",
                envelope.sagaId(), envelope.eventType(), envelope.eventId());

        if ("SendNotification".equals(envelope.eventType())) {
            UUID transferId = UUID.fromString(envelope.payload().get("transfer_id").asText());
            notifications.send(envelope.eventId(), transferId, envelope.payload());
        } else {
            log.warn("Ignoring unknown event_type={} on {}",
                    envelope.eventType(), Topics.NOTIFICATION_COMMANDS);
        }
    }
}
