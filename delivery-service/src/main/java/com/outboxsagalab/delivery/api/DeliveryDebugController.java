package com.outboxsagalab.delivery.api;

import com.outboxsagalab.delivery.domain.Delivery;
import com.outboxsagalab.delivery.domain.DeliveryRepository;
import com.outboxsagalab.delivery.domain.DeliveryStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Read-only debug endpoint for inspecting {@code deliveries} rows by order id.
 *
 * <p>Pure convenience for the lab — there is no public API contract for the
 * delivery service; the saga talks to it via Kafka only.
 */
@RestController
@RequestMapping("/deliveries")
public class DeliveryDebugController {

    private final DeliveryRepository deliveryRepository;

    public DeliveryDebugController(DeliveryRepository deliveryRepository) {
        this.deliveryRepository = deliveryRepository;
    }

    @GetMapping("/by-order/{orderId}")
    public ResponseEntity<List<DeliveryView>> byOrder(@PathVariable("orderId") UUID orderId) {
        List<DeliveryView> views = deliveryRepository.findAllByOrderId(orderId)
                .stream()
                .map(DeliveryView::from)
                .toList();
        return ResponseEntity.ok(views);
    }

    public record DeliveryView(
            UUID id,
            UUID orderId,
            String address,
            Instant scheduledAt,
            DeliveryStatus status,
            Instant createdAt,
            Instant updatedAt
    ) {
        static DeliveryView from(Delivery d) {
            return new DeliveryView(
                    d.getId(),
                    d.getOrderId(),
                    d.getAddress(),
                    d.getScheduledAt(),
                    d.getStatus(),
                    d.getCreatedAt(),
                    d.getUpdatedAt()
            );
        }
    }
}
