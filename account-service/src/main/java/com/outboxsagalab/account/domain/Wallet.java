package com.outboxsagalab.account.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Wallet aggregate. One row per (user_id, currency). Optimistic locking
 * via {@code @Version} guards against concurrent debit/credit collisions.
 */
@Entity
@Table(name = "wallet", uniqueConstraints = @UniqueConstraint(columnNames = {"user_id", "currency"}))
public class Wallet {

    @Id
    private UUID id;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "balance", nullable = false, precision = 19, scale = 4)
    private BigDecimal balance;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    protected Wallet() { /* JPA */ }

    public Wallet(UUID id, String userId, String currency, BigDecimal balance) {
        this.id = id;
        this.userId = userId;
        this.currency = currency;
        this.balance = balance;
    }

    public boolean canDebit(BigDecimal amount) {
        return balance.compareTo(amount) >= 0;
    }

    public void debit(BigDecimal amount) {
        if (!canDebit(amount)) {
            throw new IllegalStateException("Insufficient funds in wallet " + id);
        }
        this.balance = this.balance.subtract(amount);
    }

    public void credit(BigDecimal amount) {
        this.balance = this.balance.add(amount);
    }

    public UUID getId() { return id; }
    public String getUserId() { return userId; }
    public String getCurrency() { return currency; }
    public BigDecimal getBalance() { return balance; }
    public long getVersion() { return version; }
}
