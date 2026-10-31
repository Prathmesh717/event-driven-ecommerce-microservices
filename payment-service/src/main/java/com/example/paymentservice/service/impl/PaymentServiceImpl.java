package io.github.prathmesh717.paymentservice.service.impl;

import io.github.prathmesh717.contracts.event.InventoryFailedEvent;
import io.github.prathmesh717.contracts.event.OrderCreatedEvent;
import io.github.prathmesh717.contracts.event.PaymentRefundedEvent;
import io.github.prathmesh717.contracts.event.PaymentSucceededEvent;
import io.github.prathmesh717.paymentservice.entity.Payment;
import io.github.prathmesh717.paymentservice.enums.PaymentStatus;
import io.github.prathmesh717.paymentservice.producer.PaymentEventProducer;
import io.github.prathmesh717.paymentservice.repository.PaymentRepository;
import io.github.prathmesh717.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * ==========================================
 * Handles Payment Processing
 * ==========================================
 *
 * Responsibilities:
 * - Process Payments
 * - Refund Payments
 * - Publish Payment Success Events
 * - Publish Payment Refunded Events
 *
 * Patterns Implemented:
 * - Choreography-Based Saga
 * - Saga Compensation
 * - Eventual Consistency
 *
 * This service participates in the Saga workflow
 * by processing payments and handling refunds
 * when downstream inventory reservation fails.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentEventProducer paymentEventProducer;

    /**
     * Processes payment for a newly created order.
     *
     * Happy Path:
     * - Create payment record
     * - Mark payment as SUCCESS
     * - Publish PaymentSucceededEvent
     */
    @Override
    public void processPayment(
            OrderCreatedEvent orderCreatedEvent) {

        log.info(
                "Processing payment for orderId={}",
                orderCreatedEvent.orderId());

        Payment payment = Payment.builder()
                .orderId(orderCreatedEvent.orderId())
                .amount(orderCreatedEvent.amount())
                .status(PaymentStatus.SUCCESS)
                .createdAt(LocalDateTime.now())
                .build();

        payment = paymentRepository.save(payment);

        /*
         * Publish event so Inventory Service
         * can continue the Saga workflow.
         */
        PaymentSucceededEvent paymentSucceededEvent =
                new PaymentSucceededEvent(
                        UUID.randomUUID(),
                        0,
                        payment.getId(),
                        payment.getOrderId(),
                        orderCreatedEvent.productId(),
                        orderCreatedEvent.quantity(),
                        payment.getAmount());

        paymentEventProducer.publishPaymentSucceededEvent(
                paymentSucceededEvent);

        log.info(
                "Payment saved successfully for orderId={}, paymentId={}",
                payment.getOrderId(),
                payment.getId());
    }

    /**
     * Refunds payment as part of Saga compensation.
     *
     * Triggered when Inventory Service publishes
     * InventoryFailedEvent.
     *
     * Compensation Flow:
     * Inventory Service
     *      ↓
     * inventory-failed (Kafka Topic)
     *      ↓
     * Payment Service
     *      ↓
     * Refund Payment
     *      ↓
     * payment-refunded (Kafka Topic)
     */
    @Transactional
    @Override
    public void refundPayment(
            InventoryFailedEvent inventoryFailedEvent) {

        Payment payment =
                paymentRepository.findByOrderId(
                                inventoryFailedEvent.orderId())
                        .orElseThrow(() ->
                                new RuntimeException(
                                        "Payment not found. orderId="
                                                + inventoryFailedEvent.orderId()));

        payment.setStatus(
                PaymentStatus.REFUNDED);

        paymentRepository.save(payment);

        /*
         * Publish refund event so Order Service
         * can complete the compensation workflow
         * by cancelling the order.
         */
        PaymentRefundedEvent paymentRefundedEvent =
                new PaymentRefundedEvent(
                        payment.getOrderId(),
                        payment.getId(),
                        inventoryFailedEvent.reason());

        paymentEventProducer.publishPaymentRefundedEvent(
                paymentRefundedEvent);

        log.info(
                "Payment refunded successfully. orderId={}, paymentId={}",
                payment.getOrderId(),
                payment.getId());
    }
}