package com.outboxsagalab.order.api;

import com.outboxsagalab.order.domain.Order;
import com.outboxsagalab.order.domain.OrderRepository;
import com.outboxsagalab.order.domain.OrderStatus;
import com.outboxsagalab.order.saga.CommandEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.net.URI;
import java.util.UUID;

/**
 * Order-service public REST surface.
 *
 *   POST /orders        -- creates the order, kicks off the saga, returns 201.
 *   GET  /orders/{id}   -- returns the order plus its current saga state.
 *
 * The POST handler is the ONLY domain write that doesn't go through
 * {@link com.outboxsagalab.order.saga.OrderSaga}. We deliberately keep it
 * here so the entry point of the saga is visible: insert the order row +
 * the RequestPayment outbox row in a single transaction.
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderRepository orderRepository;
    private final CommandEmitter commands;

    public OrderController(OrderRepository orderRepository, CommandEmitter commands) {
        this.orderRepository = orderRepository;
        this.commands = commands;
    }

    /**
     * Single transaction:
     *   1. Persist the new order in PENDING.
     *   2. Drop a RequestPayment row into outbox.
     *   3. Move order to PAYMENT_REQUESTED.
     *
     * If any step throws, the whole thing rolls back — there is no
     * "half-created" order.
     */
    @PostMapping
    @Transactional
    public ResponseEntity<OrderResponse> create(@RequestBody CreateOrderRequest req) {
        UUID orderId = UUID.randomUUID();
        BigDecimal total = computeTotal(req);

        Order order = new Order(orderId, req.customerId(), total);
        if (req.items() != null) {
            req.items().forEach(it ->
                    order.addItem(it.sku(), it.quantity(), it.unitPrice()));
        }
        orderRepository.save(order);
        log.info("Order created saga_id={} status={} total={}", orderId, order.getStatus(), total);

        // Same-TX outbox write — this is the textbook outbox kick-off.
        commands.requestPayment(order);
        order.transitionTo(OrderStatus.PAYMENT_REQUESTED);
        log.info("Saga state {} -> {} saga_id={}",
                OrderStatus.PENDING, OrderStatus.PAYMENT_REQUESTED, orderId);

        return ResponseEntity
                .created(URI.create("/orders/" + orderId))
                .body(OrderResponse.from(order));
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> get(@PathVariable("id") UUID id) {
        return orderRepository.findById(id)
                .map(OrderResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private static BigDecimal computeTotal(CreateOrderRequest req) {
        if (req.items() == null) {
            return BigDecimal.ZERO;
        }
        return req.items().stream()
                .map(it -> it.unitPrice().multiply(BigDecimal.valueOf(it.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
