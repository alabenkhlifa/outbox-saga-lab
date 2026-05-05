package com.outboxsagalab.payment.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * Canonical JSON envelope shared by every command and event in the lab.
 *
 * <p>Shape (see docs/architecture.md section 5):
 * <pre>
 * {
 *   "event_id":    "uuid-v4",
 *   "event_type":  "PaymentApproved",
 *   "saga_id":     "uuid-v4",
 *   "occurred_at": "2026-05-05T12:34:56Z",
 *   "payload":     { ... event-specific ... }
 * }
 * </pre>
 *
 * <p>The same shape is used on both command and event topics — only
 * {@code event_type} differs. Consumers route on {@code event_type}.
 */
public record EventEnvelope(
        @JsonProperty("event_id")    UUID eventId,
        @JsonProperty("event_type")  String eventType,
        @JsonProperty("saga_id")     UUID sagaId,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("payload")     JsonNode payload
) {

    @JsonCreator
    public EventEnvelope {
        // compact constructor — fields validated by Jackson on deserialization
    }
}
