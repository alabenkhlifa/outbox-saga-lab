package com.outboxsagalab.account.outbox;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEntry, Long> {

    /**
     * Returns the next batch of unpublished outbox rows, oldest first.
     * Bounded by the {@link Pageable} so the poller never tries to drain
     * the whole backlog in a single transaction.
     */
    @Query("""
            select o from OutboxEntry o
            where o.publishedAt is null
            order by o.createdAt asc
            """)
    List<OutboxEntry> findUnpublished(Pageable pageable);
}
