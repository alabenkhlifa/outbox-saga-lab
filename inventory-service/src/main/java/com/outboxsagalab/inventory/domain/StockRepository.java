package com.outboxsagalab.inventory.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface StockRepository extends JpaRepository<Stock, String> {

    /**
     * Pessimistic SELECT FOR UPDATE on the stock row — used for the debug
     * read path. The actual reserve uses the conditional UPDATE below to keep
     * the decrement atomic.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from Stock s where s.sku = :sku")
    Optional<Stock> findForUpdate(@Param("sku") String sku);

    /**
     * Atomic conditional decrement. Returns the number of rows affected:
     *  - 1 → there was enough stock and it has been reserved.
     *  - 0 → either the SKU does not exist or available_qty < qty.
     *
     * Two concurrent ReserveStock commands for the same SKU cannot both win:
     * one of the two UPDATEs will see the already-decremented row and return 0.
     */
    @Modifying
    @Query(value = """
            UPDATE stock
               SET available_qty = available_qty - :qty,
                   updated_at    = now()
             WHERE sku = :sku
               AND available_qty >= :qty
            """, nativeQuery = true)
    int tryDecrement(@Param("sku") String sku, @Param("qty") int qty);

    /** Unconditional increment used during release/compensation. */
    @Modifying
    @Query(value = """
            UPDATE stock
               SET available_qty = available_qty + :qty,
                   updated_at    = now()
             WHERE sku = :sku
            """, nativeQuery = true)
    int increment(@Param("sku") String sku, @Param("qty") int qty);
}
