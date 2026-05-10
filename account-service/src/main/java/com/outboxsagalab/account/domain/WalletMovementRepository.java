package com.outboxsagalab.account.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface WalletMovementRepository extends JpaRepository<WalletMovement, UUID> {
}
