package com.outboxsagalab.notification.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.outboxsagalab.notification.idempotency.ProcessedEvent;
import com.outboxsagalab.notification.idempotency.ProcessedEventRepository;
import com.outboxsagalab.notification.messaging.Topics;
import com.outboxsagalab.notification.outbox.OutboxEntry;
import com.outboxsagalab.notification.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Handles {@code SendNotification} commands. Records two notification rows
 * (one for sender, one for recipient) and emits a single NotificationSent
 * reply. No real push — recording the intent is the pattern demonstration.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository notifications;
    private final OutboxRepository outbox;
    private final ProcessedEventRepository processedEvents;
    private final ObjectMapper json;

    public NotificationService(NotificationRepository notifications,
                               OutboxRepository outbox,
                               ProcessedEventRepository processedEvents,
                               ObjectMapper json) {
        this.notifications = notifications;
        this.outbox = outbox;
        this.processedEvents = processedEvents;
        this.json = json;
    }

    @Transactional
    public void send(UUID inboundEventId, UUID transferId, JsonNode payload) {
        if (processedEvents.existsById(inboundEventId)) {
            log.info("Skipping already-processed SendNotification event_id={} saga_id={}",
                    inboundEventId, transferId);
            return;
        }

        String senderUser = payload.get("sender_user").asText();
        String recipientUser = payload.get("recipient_user").asText();

        notifications.save(new Notification(UUID.randomUUID(), senderUser,
                NotificationKind.TRANSFER_SENT, payload, transferId));
        notifications.save(new Notification(UUID.randomUUID(), recipientUser,
                NotificationKind.TRANSFER_RECEIVED, payload, transferId));

        processedEvents.save(new ProcessedEvent(inboundEventId, "SendNotification"));

        ObjectNode reply = json.createObjectNode();
        reply.put("transfer_id", transferId.toString());
        reply.put("sender_user", senderUser);
        reply.put("recipient_user", recipientUser);
        enqueue(transferId, "NotificationSent", reply);

        log.info("Notifications sent transfer_id={} sender={} recipient={}",
                transferId, senderUser, recipientUser);
    }

    private void enqueue(UUID sagaId, String eventType, ObjectNode innerPayload) {
        UUID outboundEventId = UUID.randomUUID();
        ObjectNode envelope = json.createObjectNode();
        envelope.put("event_id", outboundEventId.toString());
        envelope.put("event_type", eventType);
        envelope.put("saga_id", sagaId.toString());
        envelope.put("occurred_at", Instant.now().toString());
        envelope.set("payload", innerPayload);

        String body;
        try {
            body = json.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize outbox envelope", e);
        }

        outbox.save(new OutboxEntry(outboundEventId, sagaId.toString(),
                Topics.NOTIFICATION_EVENTS, eventType, body));
        log.info("Enqueued outbox event saga_id={} event_type={} event_id={}",
                sagaId, eventType, outboundEventId);
    }
}
