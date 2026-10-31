package io.github.prathmesh717.orderservice.enums;

/**
 * Represents current state of order.
 */
public enum OrderStatus {

    CREATED,

    PAYMENT_PENDING,

    PAYMENT_SUCCESS,

    PAYMENT_FAILED,

    COMPLETED,

    REFUNDED,

    CANCELLED
}