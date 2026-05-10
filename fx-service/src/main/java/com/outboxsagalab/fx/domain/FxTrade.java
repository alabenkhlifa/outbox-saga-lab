package com.outboxsagalab.fx.domain;

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
 * One row per executed FX leg. The compensation step inserts a paired
 * REVERSED row with the same correlation_id.
 */
@Entity
@Table(name = "fx_trade")
public class FxTrade {

    @Id
    private UUID id;

    @Column(name = "base_currency", nullable = false, length = 3)
    private String baseCurrency;

    @Column(name = "quote_currency", nullable = false, length = 3)
    private String quoteCurrency;

    @Column(name = "base_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal baseAmount;

    @Column(name = "quote_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal quoteAmount;

    @Column(name = "rate", nullable = false, precision = 19, scale = 8)
    private BigDecimal rate;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private FxTradeStatus status;

    @Column(name = "correlation_id", nullable = false)
    private UUID correlationId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected FxTrade() { /* JPA */ }

    public FxTrade(UUID id, String base, String quote,
                   BigDecimal baseAmount, BigDecimal quoteAmount, BigDecimal rate,
                   FxTradeStatus status, UUID correlationId) {
        this.id = id;
        this.baseCurrency = base;
        this.quoteCurrency = quote;
        this.baseAmount = baseAmount;
        this.quoteAmount = quoteAmount;
        this.rate = rate;
        this.status = status;
        this.correlationId = correlationId;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getBaseCurrency() { return baseCurrency; }
    public String getQuoteCurrency() { return quoteCurrency; }
    public BigDecimal getBaseAmount() { return baseAmount; }
    public BigDecimal getQuoteAmount() { return quoteAmount; }
    public BigDecimal getRate() { return rate; }
    public FxTradeStatus getStatus() { return status; }
    public UUID getCorrelationId() { return correlationId; }
    public Instant getCreatedAt() { return createdAt; }
}
