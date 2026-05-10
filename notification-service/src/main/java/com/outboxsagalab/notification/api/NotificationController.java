package com.outboxsagalab.notification.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.outboxsagalab.notification.domain.Notification;
import com.outboxsagalab.notification.domain.NotificationKind;
import com.outboxsagalab.notification.domain.NotificationRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private final NotificationRepository notifications;

    public NotificationController(NotificationRepository notifications) {
        this.notifications = notifications;
    }

    public record NotificationView(UUID id, String userId, NotificationKind kind,
                                   JsonNode payload, UUID correlationId, Instant createdAt) {
        static NotificationView from(Notification n) {
            return new NotificationView(n.getId(), n.getUserId(), n.getKind(),
                    n.getPayloadJson(), n.getCorrelationId(), n.getCreatedAt());
        }
    }

    @GetMapping("/{user}")
    public List<NotificationView> forUser(@PathVariable("user") String user) {
        return notifications.findByUserIdOrderByCreatedAtDesc(user).stream()
                .map(NotificationView::from)
                .toList();
    }
}
