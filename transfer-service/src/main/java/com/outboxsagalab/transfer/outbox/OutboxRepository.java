package com.outboxsagalab.transfer.outbox;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEntry, Long> {

    /**
     * Pulls the oldest unpublished rows in insertion order. The poller
     * orders by id (BIGSERIAL) to preserve emission order within a single
     * aggregate stream.
     */
    @Query("""
            SELECT e FROM OutboxEntry e
            WHERE e.publishedAt IS NULL
            ORDER BY e.id ASC
            """)
    List<OutboxEntry> findUnpublished(Pageable pageable);
}
