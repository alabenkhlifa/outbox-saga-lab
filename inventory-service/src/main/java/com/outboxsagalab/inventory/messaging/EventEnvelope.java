package com.outboxsagalab.inventory.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * Wire format for every Kafka message in the lab — both commands and events.
 *
 * Matches the JSON shape pinned in docs/architecture.md §5:
 * <pre>
 * {
 *   "event_id":   "uuid-v4",
 *   "event_type": "ReserveStock",
 *   "saga_id":    "uuid-v4",
 *   "occurred_at":"2026-05-05T12:34:56Z",
 *   "payload":    { ... event-specific ... }
 * }
 * </pre>
 *
 * The {@code payload} is left as a raw JsonNode so each handler can deserialize
 * into its own type — services intentionally do not share DTOs.
 */
public record EventEnvelope(
        @JsonProperty("event_id")    UUID eventId,
        @JsonProperty("event_type")  String eventType,
        @JsonProperty("saga_id")     UUID sagaId,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("payload")     JsonNode payload
) {
}
