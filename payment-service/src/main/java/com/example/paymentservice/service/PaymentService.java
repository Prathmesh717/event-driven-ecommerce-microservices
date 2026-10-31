package io.github.prathmesh717.paymentservice.service;

import io.github.prathmesh717.contracts.event.InventoryFailedEvent;
import io.github.prathmesh717.contracts.event.OrderCreatedEvent;

public interface PaymentService {
    void processPayment(OrderCreatedEvent event);
    void refundPayment(InventoryFailedEvent event);
}