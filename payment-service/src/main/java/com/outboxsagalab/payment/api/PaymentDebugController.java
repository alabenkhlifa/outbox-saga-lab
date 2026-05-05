package com.outboxsagalab.payment.api;

import com.outboxsagalab.payment.domain.Payment;
import com.outboxsagalab.payment.domain.PaymentRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Debug endpoint for poking at payment rows during the lab.
 *
 * <p>Not part of the saga contract — the saga is driven entirely by Kafka
 * messages. This is here so you can curl the service and see what it has
 * recorded for a given order without having to {@code psql} into the DB.
 */
@RestController
@RequestMapping("/payments")
public class PaymentDebugController {

    private final PaymentRepository payments;

    public PaymentDebugController(PaymentRepository payments) {
        this.payments = payments;
    }

    @GetMapping("/by-order/{orderId}")
    public List<PaymentView> byOrder(@PathVariable UUID orderId) {
        return payments.findByOrderId(orderId).stream()
                .map(PaymentView::from)
                .toList();
    }

    public record PaymentView(
            UUID id,
            UUID orderId,
            BigDecimal amount,
            String currency,
            String status,
            Instant createdAt,
            Instant updatedAt
    ) {
        static PaymentView from(Payment p) {
            return new PaymentView(
                    p.getId(),
                    p.getOrderId(),
                    p.getAmount(),
                    p.getCurrency(),
                    p.getStatus().name(),
                    p.getCreatedAt(),
                    p.getUpdatedAt()
            );
        }
    }
}
