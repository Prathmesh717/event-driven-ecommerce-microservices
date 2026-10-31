package io.github.prathmesh717.notificationservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.PaymentRefundedEvent;
import io.github.prathmesh717.notificationservice.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * ==========================================
 * Handles Payment Refunded Events
 * ==========================================
 *
 * Responsibilities:
 * - Consume PaymentRefundedEvent
 * - Send Order Cancellation Notifications
 *
 * Patterns Implemented:
 * - Event-Driven Communication
 * - Eventual Consistency
 *
 * This consumer listens for payment refund events
 * generated during Saga compensation and notifies
 * customers that their order has been cancelled.
 *
 * Flow:
 *
 * Inventory Service
 *      ↓
 * inventory-failed (Kafka Topic)
 *      ↓
 * Payment Service
 *      ↓
 * payment-refunded (Kafka Topic)
 *      ↓
 * Notification Service
 *      ↓
 * Send Order Cancelled Notification
 */
@Component
@RequiredArgsConstructor
public class PaymentRefundedConsumer {

    private final NotificationService notificationService;

    /**
     * Consumes PaymentRefundedEvent and sends
     * an order cancellation notification.
     */
    @KafkaListener(
            topics = KafkaTopics.PAYMENT_REFUNDED,
            groupId = "notification-service-group",
            containerFactory =
                    "paymentRefundedKafkaListenerContainerFactory"
    )
    public void consume(
            PaymentRefundedEvent event) {

        /*
         * Notify customer that the order
         * has been cancelled and payment
         * has been refunded.
         */
        notificationService
                .sendOrderCancelledNotification(
                        event);
    }
}