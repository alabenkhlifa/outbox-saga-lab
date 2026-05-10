package com.outboxsagalab.notification.domain;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * Append-only notification record. The system records the intent to notify;
 * no real push delivery is wired up. {@code @JdbcTypeCode(SqlTypes.JSON)} is
 * the Hibernate-6 native way to map a JsonNode to a Postgres JSONB column —
 * no third-party hypersistence-utils dependency required.
 */
@Entity
@Table(name = "notification")
public class Notification {

    @Id
    private UUID id;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Enumerated(EnumType.STRING)
    @Column(name = "kind", nullable = false, length = 32)
    private NotificationKind kind;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload_json", nullable = false, columnDefinition = "jsonb")
    private JsonNode payloadJson;

    @Column(name = "correlation_id", nullable = false)
    private UUID correlationId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected Notification() { /* JPA */ }

    public Notification(UUID id, String userId, NotificationKind kind,
                        JsonNode payloadJson, UUID correlationId) {
        this.id = id;
        this.userId = userId;
        this.kind = kind;
        this.payloadJson = payloadJson;
        this.correlationId = correlationId;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getUserId() { return userId; }
    public NotificationKind getKind() { return kind; }
    public JsonNode getPayloadJson() { return payloadJson; }
    public UUID getCorrelationId() { return correlationId; }
    public Instant getCreatedAt() { return createdAt; }
}
