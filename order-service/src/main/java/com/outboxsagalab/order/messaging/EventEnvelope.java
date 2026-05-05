package com.outboxsagalab.order.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * The Kafka message envelope. EVERY command and EVERY event uses this shape.
 *
 * Defined in docs/architecture.md - section 5 "Event envelope":
 *
 *   {
 *     "event_id":   "uuid-v4",
 *     "event_type": "PaymentApproved",
 *     "saga_id":    "uuid-v4",
 *     "occurred_at": "2026-05-05T12:34:56Z",
 *     "payload":    { ... event-specific ... }
 *   }
 *
 * - event_id: unique per emission, used for idempotency
 * - saga_id:  equals the order id for correlation
 * - event_type: discriminator consumers route on
 * - payload: the actual business data (kept as JsonNode so each consumer
 *            decides how to interpret it)
 */
public record EventEnvelope(
        @JsonProperty("event_id")   UUID eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("saga_id")    UUID sagaId,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("payload")    JsonNode payload
) {
}
