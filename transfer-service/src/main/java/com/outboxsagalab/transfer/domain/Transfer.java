package com.outboxsagalab.transfer.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Saga aggregate root for a cross-currency P2P money transfer.
 *
 * One row per transfer. The {@code state} column is the saga state machine;
 * see {@code docs/architecture.md} §2.
 */
@Entity
@Table(name = "transfer")
public class Transfer {

    @Id
    private UUID id;

    @Column(name = "sender_user", nullable = false)
    private String senderUser;

    @Column(name = "sender_currency", nullable = false, length = 3)
    private String senderCurrency;

    @Column(name = "recipient_user", nullable = false)
    private String recipientUser;

    @Column(name = "recipient_currency", nullable = false, length = 3)
    private String recipientCurrency;

    @Column(name = "source_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal sourceAmount;

    @Column(name = "target_amount", precision = 19, scale = 4)
    private BigDecimal targetAmount;

    @Column(name = "rate", precision = 19, scale = 8)
    private BigDecimal rate;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 32)
    private TransferState state;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Transfer() { /* JPA */ }

    public Transfer(UUID id,
                    String senderUser, String senderCurrency,
                    String recipientUser, String recipientCurrency,
                    BigDecimal sourceAmount) {
        this.id = id;
        this.senderUser = senderUser;
        this.senderCurrency = senderCurrency;
        this.recipientUser = recipientUser;
        this.recipientCurrency = recipientCurrency;
        this.sourceAmount = sourceAmount;
        this.state = TransferState.PENDING;
        this.createdAt = Instant.now();
    }

    public void transitionTo(TransferState next) {
        this.state = next;
    }

    public void recordFx(BigDecimal targetAmount, BigDecimal rate) {
        this.targetAmount = targetAmount;
        this.rate = rate;
    }

    public UUID getId() { return id; }
    public String getSenderUser() { return senderUser; }
    public String getSenderCurrency() { return senderCurrency; }
    public String getRecipientUser() { return recipientUser; }
    public String getRecipientCurrency() { return recipientCurrency; }
    public BigDecimal getSourceAmount() { return sourceAmount; }
    public BigDecimal getTargetAmount() { return targetAmount; }
    public BigDecimal getRate() { return rate; }
    public TransferState getState() { return state; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
