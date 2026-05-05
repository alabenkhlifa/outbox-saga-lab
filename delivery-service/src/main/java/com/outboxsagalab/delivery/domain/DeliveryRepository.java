package com.outboxsagalab.delivery.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DeliveryRepository extends JpaRepository<Delivery, UUID> {

    Optional<Delivery> findFirstByOrderIdOrderByCreatedAtDesc(UUID orderId);

    List<Delivery> findAllByOrderId(UUID orderId);
}
