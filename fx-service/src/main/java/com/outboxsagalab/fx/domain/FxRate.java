package com.outboxsagalab.fx.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.IdClass;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

/**
 * Seeded fixed cross-rate. Read-only at runtime.
 */
@Entity
@Table(name = "fx_rate")
@IdClass(FxRate.PK.class)
public class FxRate {

    @Id
    @Column(name = "base_currency", length = 3)
    private String baseCurrency;

    @Id
    @Column(name = "quote_currency", length = 3)
    private String quoteCurrency;

    @Column(name = "rate", nullable = false, precision = 19, scale = 8)
    private BigDecimal rate;

    protected FxRate() { /* JPA */ }

    public FxRate(String baseCurrency, String quoteCurrency, BigDecimal rate) {
        this.baseCurrency = baseCurrency;
        this.quoteCurrency = quoteCurrency;
        this.rate = rate;
    }

    public String getBaseCurrency() { return baseCurrency; }
    public String getQuoteCurrency() { return quoteCurrency; }
    public BigDecimal getRate() { return rate; }

    public static class PK implements Serializable {
        private String baseCurrency;
        private String quoteCurrency;

        public PK() { }

        public PK(String baseCurrency, String quoteCurrency) {
            this.baseCurrency = baseCurrency;
            this.quoteCurrency = quoteCurrency;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PK pk)) return false;
            return Objects.equals(baseCurrency, pk.baseCurrency)
                    && Objects.equals(quoteCurrency, pk.quoteCurrency);
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseCurrency, quoteCurrency);
        }
    }
}
