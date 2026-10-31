package io.github.prathmesh717.paymentservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.OrderCreatedEvent;
import io.github.prathmesh717.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * ==========================================
 * Handles Order Created Events
 * ==========================================
 *
 * Responsibilities:
 * - Consume OrderCreatedEvent
 * - Initiate Payment Processing
 *
 * Patterns Implemented:
 * - Choreography-Based Saga
 * - Eventual Consistency
 *
 * This consumer listens for newly created orders
 * and starts the payment processing workflow.
 *
 * Flow:
 *
 * Order Service
 *      ↓
 * order-created (Kafka Topic)
 *      ↓
 * Payment Service
 *      ↓
 * Process Payment
 *      ↓
 * payment-succeeded (Kafka Topic)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreatedConsumer {

    private final PaymentService paymentService;

    /**
     * Consumes OrderCreatedEvent and initiates
     * payment processing.
     */
    @KafkaListener(
            topics = KafkaTopics.ORDER_CREATED,
            groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            OrderCreatedEvent event) {

        log.info(
                "Received OrderCreatedEvent orderId={}",
                event.orderId());

        /*
         * Start payment processing for the
         * newly created order.
         */
        paymentService.processPayment(event);
    }
}