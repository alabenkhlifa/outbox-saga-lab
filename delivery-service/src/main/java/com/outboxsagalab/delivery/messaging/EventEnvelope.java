package com.outboxsagalab.delivery.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Wire shape of every Kafka message — both commands and events.
 *
 * <p>Mirrors the JSON contract in {@code docs/architecture.md}:
 * <pre>
 * {
 *   "event_id":   "uuid-v4",
 *   "event_type": "ScheduleDelivery",
 *   "saga_id":    "uuid-v4",
 *   "occurred_at": "2026-05-05T12:34:56Z",
 *   "payload":    { ... event-specific ... }
 * }
 * </pre>
 *
 * <p>Same envelope on command topics and event topics — only {@code event_type}
 * (and the {@code payload} contents) differ.
 */
public record EventEnvelope(
        @JsonProperty("event_id")    UUID eventId,
        @JsonProperty("event_type")  String eventType,
        @JsonProperty("saga_id")     UUID sagaId,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("payload")     Map<String, Object> payload
) {
}
