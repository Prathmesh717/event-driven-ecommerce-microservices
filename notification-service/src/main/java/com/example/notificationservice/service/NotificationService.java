package io.github.prathmesh717.notificationservice.service;

import io.github.prathmesh717.contracts.event.InventoryReservedEvent;
import io.github.prathmesh717.contracts.event.PaymentRefundedEvent;

public interface NotificationService {

    void sendOrderCompletedNotification(InventoryReservedEvent event);

    void sendOrderCancelledNotification(PaymentRefundedEvent event);
}