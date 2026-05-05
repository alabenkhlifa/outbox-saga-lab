package com.outboxsagalab.inventory.outbox;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEntry, Long> {

    /**
     * Reads a batch of unpublished rows in insertion order. The poller calls
     * this in its own transaction, then publishes + marks each row sent.
     */
    @Query("""
            select o
              from OutboxEntry o
             where o.publishedAt is null
             order by o.createdAt asc, o.id asc
            """)
    List<OutboxEntry> findUnpublished(Pageable pageable);
}
