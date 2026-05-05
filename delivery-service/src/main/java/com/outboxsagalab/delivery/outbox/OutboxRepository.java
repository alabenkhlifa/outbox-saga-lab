package com.outboxsagalab.delivery.outbox;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEntry, Long> {

    /**
     * Returns the oldest unpublished rows in insertion order. Used by the
     * scheduled poller — see {@link OutboxPublisher}.
     */
    @Query("SELECT o FROM OutboxEntry o WHERE o.publishedAt IS NULL ORDER BY o.createdAt ASC, o.id ASC")
    List<OutboxEntry> findUnpublished(Pageable pageable);
}
