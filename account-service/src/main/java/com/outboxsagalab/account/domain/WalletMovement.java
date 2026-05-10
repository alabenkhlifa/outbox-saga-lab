package com.outboxsagalab.account.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Append-only journal of wallet changes. One row per applied debit/credit/refund.
 */
@Entity
@Table(name = "wallet_movement")
public class WalletMovement {

    @Id
    private UUID id;

    @Column(name = "wallet_id", nullable = false)
    private UUID walletId;

    @Column(name = "amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;     // always positive; direction is `type`

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, length = 16)
    private WalletMovementType type;

    @Column(name = "correlation_id", nullable = false)
    private UUID correlationId;     // = transfer id

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected WalletMovement() { /* JPA */ }

    public WalletMovement(UUID id, UUID walletId, BigDecimal amount,
                          WalletMovementType type, UUID correlationId) {
        this.id = id;
        this.walletId = walletId;
        this.amount = amount;
        this.type = type;
        this.correlationId = correlationId;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public UUID getWalletId() { return walletId; }
    public BigDecimal getAmount() { return amount; }
    public WalletMovementType getType() { return type; }
    public UUID getCorrelationId() { return correlationId; }
    public Instant getCreatedAt() { return createdAt; }
}
