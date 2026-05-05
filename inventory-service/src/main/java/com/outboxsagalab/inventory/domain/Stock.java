package com.outboxsagalab.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.OffsetDateTime;

/**
 * Per-SKU stock row. The seed migration (V2) inserts a few example items.
 * Decremented atomically by InventoryService when reservations succeed.
 */
@Entity
@Table(name = "stock")
public class Stock {

    @Id
    @Column(name = "sku", nullable = false, length = 64)
    private String sku;

    @Column(name = "available_qty", nullable = false)
    private int availableQty;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Stock() {
    }

    public Stock(String sku, int availableQty) {
        this.sku = sku;
        this.availableQty = availableQty;
        this.updatedAt = OffsetDateTime.now();
    }

    public String getSku() {
        return sku;
    }

    public int getAvailableQty() {
        return availableQty;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
}
